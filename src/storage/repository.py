"""
Write-side data access for the scrape pipeline.

Business rules implemented here (each one is covered by tests):

1. Price periods: an observation extends the open period of its listing when
   the price, regular price, currency and stock state are unchanged and the
   listing was still active (i.e. it was not missing from any successful run
   since it was last seen). Otherwise a new period starts. Re-running the
   scrape on the same day is therefore idempotent.
2. Delisting: after a successful run of (store, category), listings of that
   store/category that were not seen in the run become inactive.
3. Anomalies: a run with zero listings, or with fewer than half the median of
   the last successful runs, never deactivates anything (see classify_run),
   unless the drop already held for the previous runs: then it is the store's
   new size, not a broken scrape.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
import urllib.parse
from dataclasses import dataclass
from datetime import datetime

from src.domain.models import Category, ScrapedListing
from src.settings import to_iso

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "srsltid",
}

ANOMALY_RATIO = 0.5
ANOMALY_WINDOW = 7
NEW_LEVEL_RUNS = 3  # runs at the same lower count that make it the new normal
NEW_LEVEL_TOLERANCE = 0.1


def canonical_url(url: str) -> str:
    """Stable identity for a product URL: lowercase host, no fragment/tracking params."""
    parsed = urllib.parse.urlsplit(url.strip())
    query = [
        (k, v)
        for k, v in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    ]
    return urllib.parse.urlunsplit(
        (
            (parsed.scheme or "https").lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            urllib.parse.urlencode(query),
            "",
        )
    )


# ---------------------------------------------------------------------------
# Stores and scrape runs
# ---------------------------------------------------------------------------


def sync_stores(conn: sqlite3.Connection, stores: list[tuple[str, str, str, str]]) -> None:
    """Upsert (id, name, base_url, platform) rows."""
    with conn:
        conn.executemany(
            """
            INSERT INTO stores (id, name, base_url, platform) VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = excluded.name, base_url = excluded.base_url, platform = excluded.platform
            """,
            stores,
        )


def start_scrape_run(
    conn: sqlite3.Connection, run_id: str, store_id: str, category: Category, started_at: datetime
) -> int:
    with conn:
        cur = conn.execute(
            """
            INSERT INTO scrape_runs (run_id, store_id, category, started_at, status)
            VALUES (?, ?, ?, ?, 'running')
            """,
            (run_id, store_id, category.value, to_iso(started_at)),
        )
    assert cur.lastrowid is not None
    return cur.lastrowid


def finish_scrape_run(
    conn: sqlite3.Connection,
    scrape_run_id: int,
    *,
    status: str,
    finished_at: datetime,
    n_found: int = 0,
    n_new: int = 0,
    n_changed: int = 0,
    n_delisted: int = 0,
    error: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE scrape_runs
            SET status = ?, finished_at = ?, n_found = ?, n_new = ?, n_changed = ?,
                n_delisted = ?, error = ?
            WHERE id = ?
            """,
            (
                status,
                to_iso(finished_at),
                n_found,
                n_new,
                n_changed,
                n_delisted,
                error,
                scrape_run_id,
            ),
        )


def classify_run(conn: sqlite3.Connection, store_id: str, category: Category, n_found: int) -> str:
    """Return 'ok', 'empty' or 'anomaly' for a scrape that finished without errors."""
    if n_found == 0:
        return "empty"
    rows = conn.execute(
        """
        SELECT n_found FROM scrape_runs
        WHERE store_id = ? AND category = ? AND status = 'ok'
        ORDER BY started_at DESC LIMIT ?
        """,
        (store_id, category.value, ANOMALY_WINDOW),
    ).fetchall()
    if rows:
        median = statistics.median(r[0] for r in rows)
        if n_found < ANOMALY_RATIO * median and not _held_level(conn, store_id, category, n_found):
            return "anomaly"
    return "ok"


def _held_level(conn: sqlite3.Connection, store_id: str, category: Category, n_found: int) -> bool:
    """True when the previous runs already found about ``n_found`` listings."""
    rows = conn.execute(
        """
        SELECT n_found FROM scrape_runs
        WHERE store_id = ? AND category = ? AND status IN ('ok', 'anomaly')
        ORDER BY started_at DESC LIMIT ?
        """,
        (store_id, category.value, NEW_LEVEL_RUNS - 1),
    ).fetchall()
    return len(rows) == NEW_LEVEL_RUNS - 1 and all(
        abs(r[0] - n_found) <= NEW_LEVEL_TOLERANCE * n_found for r in rows
    )


# ---------------------------------------------------------------------------
# Listings and price periods
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObservationResult:
    listing_id: int
    is_new_listing: bool
    price_changed: bool


def record_observation(
    conn: sqlite3.Connection, listing: ScrapedListing, price_usd: float
) -> ObservationResult:
    """
    Upsert the listing and extend or open its price period (rule 1).

    Must be called inside a transaction managed by the caller.
    """
    url = canonical_url(listing.url)
    seen = to_iso(listing.scraped_at)
    row = conn.execute(
        "SELECT id, is_active FROM listings WHERE store_id = ? AND url = ?",
        (listing.store_id, url),
    ).fetchone()

    if row is None:
        cur = conn.execute(
            """
            INSERT INTO listings (
                store_id, url, store_sku, title, category, brand, image_url,
                first_seen_at, last_seen_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                listing.store_id,
                url,
                listing.store_sku,
                listing.title,
                listing.category.value,
                listing.brand,
                listing.image_url,
                seen,
                seen,
            ),
        )
        assert cur.lastrowid is not None
        listing_id, was_active, is_new = cur.lastrowid, False, True
    else:
        listing_id, was_active, is_new = row["id"], bool(row["is_active"]), False
        # A listing that moves to another category must be resolved again with
        # that category's parser: clearing resolver_version queues it.
        conn.execute(
            """
            UPDATE listings SET
                title = ?, store_sku = COALESCE(?, store_sku),
                resolver_version = CASE WHEN category = ? THEN resolver_version END,
                category = ?,
                brand = COALESCE(?, brand), image_url = COALESCE(?, image_url),
                last_seen_at = ?, is_active = 1
            WHERE id = ?
            """,
            (
                listing.title,
                listing.store_sku,
                listing.category.value,
                listing.category.value,
                listing.brand,
                listing.image_url,
                seen,
                listing_id,
            ),
        )

    in_stock = None if listing.in_stock is None else int(listing.in_stock)
    open_period = conn.execute(
        """
        SELECT id, price, regular_price, currency, in_stock, last_seen FROM price_periods
        WHERE listing_id = ? ORDER BY valid_from DESC, id DESC LIMIT 1
        """,
        (listing_id,),
    ).fetchone()

    unchanged = (
        open_period is not None
        and was_active
        and open_period["price"] == listing.price
        and open_period["regular_price"] == listing.regular_price
        and open_period["currency"] == listing.currency.value
        and open_period["in_stock"] == in_stock
    )
    if unchanged:
        assert open_period is not None
        if seen > open_period["last_seen"]:
            conn.execute(
                "UPDATE price_periods SET last_seen = ? WHERE id = ?", (seen, open_period["id"])
            )
        return ObservationResult(listing_id, is_new, price_changed=False)

    conn.execute(
        """
        INSERT INTO price_periods (
            listing_id, price, regular_price, currency, price_usd, in_stock, valid_from, last_seen
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing_id,
            listing.price,
            listing.regular_price,
            listing.currency.value,
            round(price_usd, 2),
            in_stock,
            seen,
            seen,
        ),
    )
    price_changed = open_period is not None and (
        open_period["price"] != listing.price or open_period["in_stock"] != in_stock
    )
    return ObservationResult(listing_id, is_new, price_changed=price_changed)


def unseen_active_urls(
    conn: sqlite3.Connection, store_id: str, category: Category, seen_ids: set[int], limit: int
) -> list[str]:
    """A random sample of the active listings of the store/category missing from this run."""
    rows = conn.execute(
        """
        SELECT url FROM listings
        WHERE store_id = ? AND category = ? AND is_active = 1
          AND id NOT IN (SELECT value FROM json_each(?))
        ORDER BY RANDOM() LIMIT ?
        """,
        (store_id, category.value, json.dumps(sorted(seen_ids)), limit),
    ).fetchall()
    return [r[0] for r in rows]


def deactivate_unseen(
    conn: sqlite3.Connection, store_id: str, category: Category, seen_ids: set[int]
) -> int:
    """Rule 2: listings of the store/category not seen in this run are no longer published."""
    cur = conn.execute(
        """
        UPDATE listings SET is_active = 0
        WHERE store_id = ? AND category = ? AND is_active = 1
          AND id NOT IN (SELECT value FROM json_each(?))
        """,
        (store_id, category.value, json.dumps(sorted(seen_ids))),
    )
    return cur.rowcount


# ---------------------------------------------------------------------------
# FX rates
# ---------------------------------------------------------------------------


def get_fx_rate(conn: sqlite3.Connection, day: str) -> float | None:
    """USD/UYU rate for a local date, falling back to the latest earlier rate."""
    row = conn.execute(
        "SELECT usd_uyu FROM fx_rates WHERE date <= ? ORDER BY date DESC LIMIT 1", (day,)
    ).fetchone()
    return float(row[0]) if row else None


def save_fx_rate(
    conn: sqlite3.Connection, day: str, rate: float, source: str, fetched_at: datetime
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO fx_rates (date, usd_uyu, source, fetched_at) VALUES (?, ?, ?, ?)
            ON CONFLICT (date) DO UPDATE SET
                usd_uyu = excluded.usd_uyu, source = excluded.source,
                fetched_at = excluded.fetched_at
            """,
            (day, rate, source, to_iso(fetched_at)),
        )
