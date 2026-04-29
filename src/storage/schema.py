"""
Database schema for hardware-pulse.

Responsibilities:
- Define DDL for all tables (CREATE TABLE, indexes, constraints)
- Initialize the database on first run
- Idempotent: safe to call on an existing database

Does NOT:
- Insert, update, or query data
- Contain business logic
"""

import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_RAW_LISTINGS = """
CREATE TABLE IF NOT EXISTS raw_listings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Deterministic deduplication key
    -- Computed as: sha256(source + ":" + item_id_or_url)
    listing_key     TEXT NOT NULL UNIQUE,

    -- Source identity
    source          TEXT NOT NULL,
    item_id         TEXT,               -- MercadoLibre only
    url             TEXT NOT NULL,
    timestamp       TEXT NOT NULL,      -- ISO 8601 UTC

    -- Core listing fields
    title           TEXT NOT NULL,
    price           REAL NOT NULL,
    currency        TEXT NOT NULL,
    seller          TEXT NOT NULL,

    -- Optional fields
    condition       TEXT,
    available_quantity INTEGER,
    base_price      REAL,

    -- Audit
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

# Indexes are defined separately from the table.
# listing_key already has a UNIQUE constraint (which implies an index),
# but we add explicit indexes for the queries we know we'll run often.
CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_raw_listings_source
    ON raw_listings (source);

CREATE INDEX IF NOT EXISTS idx_raw_listings_timestamp
    ON raw_listings (timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_listings_source_timestamp
    ON raw_listings (source, timestamp);
"""

CREATE_PRICE_SNAPSHOT_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_price_snapshots_product
    ON price_snapshots (canonical_product_id);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_timestamp
    ON price_snapshots (timestamp);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_product_timestamp
    ON price_snapshots (canonical_product_id, timestamp);
"""

CREATE_PRICE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS price_snapshots (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp             TEXT NOT NULL,
    canonical_product_id  TEXT NOT NULL,
    source                TEXT NOT NULL,
    seller                TEXT NOT NULL,
    listing_id            TEXT,
    price                 REAL NOT NULL,
    currency              TEXT NOT NULL,
    price_usd             REAL NOT NULL,
    availability          INTEGER,
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


def init_db(db_path: Path) -> sqlite3.Connection:
    """
    Initialize the SQLite database at the given path.

    Creates the database file and all tables/indexes if they don't exist.
    Safe to call on an existing database — all statements use IF NOT EXISTS.

    Args:
        db_path: Path to the SQLite file (e.g. Path("data/hardware_pulse.db"))

    Returns:
        An open sqlite3.Connection with WAL mode and row_factory set.

    WAL (Write-Ahead Logging) mode allows concurrent reads while a write
    is in progress. Important when the scraper and an analysis notebook
    run at the same time.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # rows behave like dicts: row["title"]

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    # Apply schema
    conn.executescript(CREATE_RAW_LISTINGS)
    conn.executescript(CREATE_PRICE_SNAPSHOTS)
    conn.executescript(CREATE_INDEXES)
    conn.executescript(CREATE_PRICE_SNAPSHOT_INDEXES)
    conn.commit()

    return conn
