"""
Resolve stage: assign every listing to its family and exact product.

Incremental: only listings that are new, whose title changed, or that were
resolved by an older resolver version are processed (``full=True`` redoes all).

Product assignment:
- matched listings share the product with the same fingerprint; a listing
  whose manufacturer part number (MPN) is already known in the same family
  joins that product even if its title is worded differently;
- family_only listings get a product of their own (never merged by guess);
- rejected / unparsed listings get no product.
Products left without listings are deleted.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass

from src.domain.models import Category
from src.entities.resolver import RESOLVER_VERSION, Resolution, resolve_title
from src.entities.taxonomy import load_taxonomy
from src.settings import to_iso, utcnow

logger = logging.getLogger(__name__)


@dataclass
class ResolveStats:
    processed: int = 0
    matched: int = 0
    family_only: int = 0
    rejected: int = 0
    unparsed: int = 0
    products_deleted: int = 0


def resolve_listings(conn: sqlite3.Connection, *, full: bool = False) -> ResolveStats:
    taxonomy = load_taxonomy()
    query = "SELECT id, store_id, category, title, brand, image_url FROM listings"
    if not full:
        query += (
            " WHERE resolver_version IS NULL OR resolver_version != ? OR resolved_title IS NULL"
            " OR resolved_title != title"
        )
    rows = conn.execute(query, () if full else (RESOLVER_VERSION,)).fetchall()
    stats = ResolveStats()
    now = to_iso(utcnow())
    with conn:
        for row in rows:
            resolution = resolve_title(
                Category(row["category"]), row["title"], row["brand"], taxonomy
            )
            product_id = _assign_product(conn, row, resolution, now)
            conn.execute(
                """
                UPDATE listings SET product_id = ?, condition = ?, mpn = ?, match_status = ?,
                    match_notes = ?, resolver_version = ?, resolved_title = title
                WHERE id = ?
                """,
                (
                    product_id,
                    resolution.condition.value,
                    resolution.mpn,
                    resolution.status,
                    resolution.notes_text or None,
                    RESOLVER_VERSION,
                    row["id"],
                ),
            )
            stats.processed += 1
            setattr(stats, resolution.status, getattr(stats, resolution.status) + 1)
        stats.products_deleted = conn.execute(
            "DELETE FROM products WHERE id NOT IN "
            "(SELECT product_id FROM listings WHERE product_id IS NOT NULL)"
        ).rowcount
    logger.info("Resolved %s", stats)
    return stats


def _unknowns(family_id: str | None) -> int:
    """Specs a family id leaves "sin dato" (monitor panel/Hz/resolution, PSU rating)."""
    return (family_id or "").count("-na")


def _assign_product(
    conn: sqlite3.Connection, row: sqlite3.Row, resolution: Resolution, now: str
) -> int | None:
    parsed = resolution.parsed
    if parsed is None:
        return None
    conn.execute(
        """
        INSERT INTO families (id, category, name, specs_json) VALUES (?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET name = excluded.name, specs_json = excluded.specs_json
        """,
        (
            parsed.family_id,
            parsed.category.value,
            parsed.family_name,
            json.dumps(parsed.family_specs),
        ),
    )

    if parsed.fingerprint and resolution.mpn:
        same_mpn = conn.execute(
            "SELECT id FROM products WHERE mpn = ? AND family_id = ? ORDER BY id LIMIT 1",
            (resolution.mpn, parsed.family_id),
        ).fetchone()
        if same_mpn:
            return int(same_mpn["id"])

    fingerprint = parsed.fingerprint or f"listing:{row['id']}"
    existing = conn.execute(
        "SELECT id, mpn, image_url, family_id FROM products WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()
    if existing and _unknowns(parsed.family_id) > _unknowns(existing["family_id"]):
        # Another store's title said more (e.g. the monitor's panel): keep that
        # family, name and specs. Families of the first four categories never
        # have unknown parts, so they keep updating as before.
        conn.execute(
            "UPDATE products SET mpn = COALESCE(mpn, ?), image_url = COALESCE(image_url, ?),"
            " updated_at = ? WHERE id = ?",
            (resolution.mpn, row["image_url"], now, existing["id"]),
        )
        return int(existing["id"])
    if existing:
        conn.execute(
            """
            UPDATE products SET family_id = ?, name = ?, brand = ?, line = ?, specs_json = ?,
                mpn = COALESCE(mpn, ?), image_url = COALESCE(image_url, ?), updated_at = ?
            WHERE id = ?
            """,
            (
                parsed.family_id,
                parsed.name,
                parsed.brand,
                parsed.line,
                json.dumps(parsed.specs),
                resolution.mpn,
                row["image_url"],
                now,
                existing["id"],
            ),
        )
        return int(existing["id"])

    cur = conn.execute(
        """
        INSERT INTO products (
            fingerprint, category, family_id, brand, line, name, mpn, specs_json, image_url,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            fingerprint,
            parsed.category.value,
            parsed.family_id,
            parsed.brand,
            parsed.line,
            parsed.name,
            resolution.mpn if parsed.fingerprint else None,
            json.dumps(parsed.specs),
            row["image_url"],
            now,
            now,
        ),
    )
    assert cur.lastrowid is not None
    return cur.lastrowid
