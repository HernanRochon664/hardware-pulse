"""
Repository layer for hardware-pulse.

Responsibilities:
- Persist RawListing objects to SQLite
- Deduplicate via listing_key (upsert semantics)
- Expose simple query helpers for pipeline use

Does NOT:
- Define schema (see schema.py)
- Normalize or enrich data
- Contain business logic
"""

import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime

from src.domain.models import RawListing, ResolvedListing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class UpsertResult:
    id: int
    inserted: bool
    updated: bool


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def _compute_listing_key(listing: RawListing) -> str:
    """
    Derive a deterministic deduplication key for a listing.

    Strategy:
    - MercadoLibre: hash(source + ":" + item_id)
    - Retailers:    hash(source + ":" + normalized_url)

    We prefer item_id for MercadoLibre because it is stable across
    scrape runs even if the URL changes (e.g. title slug updates).
    For retailers, the URL is the only stable identifier we have.
    """
    source = listing.source.value
    identifier = listing.item_id if listing.item_id else _normalize_url(str(listing.url))
    raw = f"{source}:{identifier}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _normalize_url(url: str) -> str:
    """
    Normalize a URL to a canonical form for stable hashing.

    Removes:
    - Trailing slashes
    - UTM and tracking query params
    - Protocol and www prefix differences

    Normalization ensures that the same product URL with minor
    variations (utm_source, trailing slash) maps to the same key.
    """
    import urllib.parse

    parsed = urllib.parse.urlparse(url.lower().rstrip("/"))

    # Strip known tracking params
    TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "fbclid", "gclid"}
    query_params = urllib.parse.parse_qs(parsed.query)
    clean_params = {k: v for k, v in query_params.items() if k not in TRACKING_PARAMS}
    clean_query = urllib.parse.urlencode(clean_params, doseq=True)

    normalized = parsed._replace(query=clean_query, fragment="")
    return urllib.parse.urlunparse(normalized)


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------


def upsert_raw_listing(listing: RawListing, conn: sqlite3.Connection) -> UpsertResult:
    listing_key = _compute_listing_key(listing)
    now = datetime.now(UTC).isoformat()
    params = {
        "listing_key": listing_key,
        "source": listing.source.value,
        "item_id": listing.item_id,
        "url": str(listing.url),
        "timestamp": listing.timestamp.isoformat(),
        "title": listing.title,
        "price": listing.price,
        "currency": listing.currency.value,
        "seller": listing.seller,
        "condition": listing.condition.value if listing.condition else None,
        "available_quantity": listing.available_quantity,
        "base_price": listing.base_price,
        "now": now,
    }

    with conn:
        existing = conn.execute(
            "SELECT id, price FROM raw_listings WHERE listing_key = ?",
            (listing_key,),
        ).fetchone()

        if existing is not None and existing["price"] == listing.price:
            return UpsertResult(id=existing["id"], inserted=False, updated=False)

        cursor = conn.execute(
            """
            INSERT INTO raw_listings (
                listing_key, source, item_id, url, timestamp,
                title, price, currency, seller,
                condition, available_quantity, base_price,
                created_at, updated_at
            ) VALUES (
                :listing_key, :source, :item_id, :url, :timestamp,
                :title, :price, :currency, :seller,
                :condition, :available_quantity, :base_price,
                :now, :now
            ) ON CONFLICT (listing_key) DO UPDATE SET
                price = excluded.price,
                updated_at = excluded.updated_at
            """,
            params,
        )

        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("Invariant violated: lastrowid is None after INSERT")

        inserted = existing is None
        logger.debug(
            "%s listing_key=%s title=%r",
            "Inserted" if inserted else "Updated",
            listing_key[:8],
            listing.title,
        )

        return UpsertResult(id=row_id, inserted=inserted, updated=not inserted)


def insert_price_snapshot(resolved: ResolvedListing, conn: sqlite3.Connection) -> int:
    """Insert a resolved listing as a price snapshot. Returns the new row id."""
    if not resolved.canonical_product_id:
        raise ValueError(
            "ResolvedListing must have canonical_product_id before inserting a price snapshot."
        )

    with conn:  # context manager → auto commit or rollback
        cursor = conn.execute(
            """
            INSERT INTO price_snapshots (
                timestamp, canonical_product_id, source, seller,
                listing_id, price, currency, price_usd, availability
            ) VALUES (
                :timestamp, :canonical_product_id, :source, :seller,
                :listing_id, :price, :currency, :price_usd, :availability
            )
            """,
            {
                "timestamp": resolved.timestamp.isoformat(),
                "canonical_product_id": resolved.canonical_product_id,
                "source": resolved.source.value,
                "seller": resolved.seller,
                "listing_id": resolved.item_id,
                "price": resolved.price,
                "currency": resolved.currency.value,
                "price_usd": resolved.price_usd
                if resolved.price_usd is not None
                else resolved.price,
                "availability": resolved.available_quantity,
            },
        )

        row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("Invariant violated: lastrowid is None after inserting price snapshot")

    return row_id


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def get_latest_listings(
    conn: sqlite3.Connection,
    source: str | None = None,
    limit: int = 100,
) -> list[sqlite3.Row]:
    """
    Retrieve the most recently updated listings.

    Args:
        conn:   Open sqlite3.Connection.
        source: Optional filter by source (e.g. "mercadolibre").
        limit:  Maximum rows to return.

    Returns:
        List of sqlite3.Row objects (accessible as dicts).
    """
    if source:
        return conn.execute(
            """
            SELECT * FROM raw_listings
            WHERE source = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (source, limit),
        ).fetchall()

    return conn.execute(
        """
        SELECT * FROM raw_listings
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
