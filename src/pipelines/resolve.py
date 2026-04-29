"""
Entity resolution pipeline for hardware-pulse.

Responsibilities:
- Read raw_listings from SQLite (with optional filters)
- Resolve each listing against the canonical catalog
- Insert resolved listings as price_snapshots
- Return aggregated statistics

Does NOT:
- Scrape data (see scrapers/)
- Define schema (see storage/schema.py)
- Load the catalog (caller responsibility)
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from src.domain.models import Condition, Currency, RawListing, Source
from src.entities.catalog import Catalog
from src.entities.resolver import resolve_batch
from src.storage.repository import insert_price_snapshot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class ResolveResult:
    processed: int = 0   # total raw listings read
    resolved: int = 0    # successfully matched to canonical SKU
    skipped: int = 0     # unmatched (canonical_product_id is None)
    errors: int = 0      # exceptions during insert

    @property
    def total(self) -> int:
        return self.resolved + self.skipped + self.errors

    def __str__(self) -> str:
        return (
            f"ResolveResult("
            f"processed={self.processed}, "
            f"resolved={self.resolved}, "
            f"skipped={self.skipped}, "
            f"errors={self.errors})"
        )


# ---------------------------------------------------------------------------
# Raw listing reconstruction
# ---------------------------------------------------------------------------


def _rows_to_raw_listings(rows: list[sqlite3.Row]) -> list[RawListing]:
    """
    Reconstruct RawListing objects from sqlite3.Row results.

    We reconstruct from DB rows rather than keeping objects in memory
    across pipeline stages, this allows the resolve pipeline to run
    independently from the ingest pipeline.
    """
    listings = []
    for row in rows:
        try:
            listing = RawListing(
                source=Source(row["source"]),
                url=row["url"],
                timestamp=row["timestamp"],
                title=row["title"],
                price=float(row["price"]),
                currency=Currency(row["currency"]),
                seller=row["seller"],
                item_id=row["item_id"],
                condition=Condition(row["condition"]) if row["condition"] else None,
                available_quantity=row["available_quantity"],
                base_price=float(row["base_price"]) if row["base_price"] else None,
            )
            listings.append(listing)
        except Exception as exc:
            logger.warning("Failed to reconstruct RawListing from row id=%s: %s", row["id"], exc)

    return listings


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def _fetch_raw_listings(
    conn: sqlite3.Connection,
    source: str | None,
    since: datetime | None,
) -> list[sqlite3.Row]:
    """
    Fetch raw listings from the database with optional filters.

    Filtering by source and since allows incremental processing,
    only new or updated listings need to be resolved on each run.
    """
    query = "SELECT * FROM raw_listings WHERE 1=1"
    params: list = []

    if source:
        query += " AND source = ?"
        params.append(source)

    if since:
        query += " AND timestamp >= ?"
        params.append(since.isoformat())

    query += " ORDER BY timestamp ASC"

    return conn.execute(query, params).fetchall()


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def resolve(
    *,
    conn: sqlite3.Connection,
    catalog: Catalog,
    source: str | None = None,
    since: datetime | None = None,
    run_at: datetime | None = None,
) -> ResolveResult:
    """
    Run the entity resolution pipeline.

    Reads raw_listings from the database, resolves each listing against
    the canonical catalog, and inserts matched listings as price_snapshots.

    Args:
        conn:    Open sqlite3.Connection. Caller manages lifecycle.
        catalog: Loaded canonical catalog for matching.
        source:  Optional filter, process only listings from this source.
        since:   Optional filter, process only listings after this timestamp.
        run_at:  Timestamp for this pipeline run. Defaults to UTC now.

    Returns:
        ResolveResult with counts of processed, resolved, skipped, errors.
    """
    run_at = run_at or datetime.now(timezone.utc)
    result = ResolveResult()

    logger.info(
        "Starting resolution run at %s (source=%r, since=%r)",
        run_at.isoformat(),
        source,
        since.isoformat() if since else None,
    )

    # Step 1: Fetch raw listings from DB
    rows = _fetch_raw_listings(conn, source, since)
    logger.info("Fetched %d raw listings to resolve", len(rows))

    if not rows:
        return result

    # Step 2: Reconstruct RawListing objects
    listings = _rows_to_raw_listings(rows)
    result.processed = len(listings)

    # Step 3: Resolve batch against catalog
    resolved_listings = resolve_batch(listings, catalog)

    # Step 4: Insert resolved listings as price_snapshots
    for resolved in resolved_listings:
        if resolved.canonical_product_id is None:
            # Step 4a: Unmatched listings are skipped, not inserted into price_snapshots
            result.skipped += 1
            continue

        try:
            insert_price_snapshot(resolved, conn)
            result.resolved += 1
        except Exception as exc:
            logger.warning(
                "Failed to insert snapshot for %r: %s",
                resolved.title,
                exc,
            )
            result.errors += 1

    logger.info("Resolution complete: %s", result)
    return result