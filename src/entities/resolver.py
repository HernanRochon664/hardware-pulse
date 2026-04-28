"""
Entity resolver for hardware-pulse.

Responsibilities:
- Orchestrate matching strategies in priority order: exact → regex → fuzzy
- Produce ResolvedListing objects from RawListing inputs
- Flag unresolved listings for manual review

Does NOT:
- Load the catalog (caller responsibility)
- Persist results to database
- Modify raw listings
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.models import ResolvedListing, RawListing
from src.entities.catalog import Catalog
from src.entities.matcher import exact_match, fuzzy_match, regex_match
from src.entities.normalizer import extract_brand

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Resolution pipeline
# ---------------------------------------------------------------------------


def resolve(listing: RawListing, catalog: Catalog) -> ResolvedListing:
    """
    Resolve a RawListing to a canonical product identity.

    Applies matching strategies in order of confidence:
    1. exact_match  → confidence 1.0
    2. regex_match  → confidence 0.9
    3. fuzzy_match  → confidence ≥ threshold (default 0.8)
    4. no match     → canonical_product_id=None, flagged for review

    Args:
        listing: Raw scraped listing from any source.
        catalog: Loaded canonical catalog (dict[sku, metadata]).

    Returns:
        ResolvedListing with canonical identity and match metadata.
        canonical_product_id is None if no strategy succeeded.
    """
    title = listing.title

    # Strategy 1: exact match (substring of normalized title vs normalized SKU)
    sku, score = exact_match(title, catalog)
    if sku is not None:
        logger.debug("Exact match: %r → %r", title, sku)
        return _build_resolved(listing, catalog, sku, score, matched_by="exact")

    # Strategy 2: regex extraction + catalog validation
    sku, score = regex_match(title, catalog)
    if sku is not None:
        logger.debug("Regex match: %r → %r (score=%.2f)", title, sku, score)
        return _build_resolved(listing, catalog, sku, score, matched_by="regex")

    # Strategy 3: fuzzy similarity fallback
    sku, score = fuzzy_match(title, catalog)
    if sku is not None:
        logger.debug("Fuzzy match: %r → %r (score=%.2f)", title, sku, score)
        return _build_resolved(listing, catalog, sku, score, matched_by="fuzzy")

    # No match, flag for manual review
    logger.debug("No match: %r", title)
    return _build_resolved(listing, catalog, None, 0.0, matched_by=None)


def resolve_batch(
    listings: list[RawListing],
    catalog: Catalog,
) -> list[ResolvedListing]:
    """
    Resolve a batch of listings against the catalog.

    Args:
        listings: List of raw listings from scrapers.
        catalog:  Loaded canonical catalog.

    Returns:
        List of ResolvedListing, one per input listing, in order.
        Listings that could not be resolved have canonical_product_id=None.
    """
    resolved = [resolve(listing, catalog) for listing in listings]

    matched = sum(1 for r in resolved if r.canonical_product_id is not None)
    logger.info(
        "Resolved %d/%d listings (%.0f%%)",
        matched,
        len(listings),
        100 * matched / len(listings) if listings else 0,
    )
    return resolved


# ---------------------------------------------------------------------------
# Internal builder
# ---------------------------------------------------------------------------


def _build_resolved(
    listing: RawListing,
    catalog: Catalog,
    sku: str | None,
    confidence: float,
    matched_by: str | None,
) -> ResolvedListing:
    """
    Build a ResolvedListing from a RawListing and match results.

    Brand is extracted independently from the title, it's an AIB brand
    (ASUS, MSI, Gigabyte...) not the chip manufacturer (NVIDIA, AMD).
    The chip manufacturer comes from the catalog via brand_family.
    """
    brand = extract_brand(listing.title)

    return ResolvedListing(
        # Traceability fields from RawListing
        source=listing.source,
        url=str(listing.url),
        timestamp=listing.timestamp,
        title=listing.title,
        price=listing.price,
        currency=listing.currency,
        seller=listing.seller,
        item_id=listing.item_id,
        condition=listing.condition,
        available_quantity=listing.available_quantity,
        base_price=listing.base_price,
        # Entity resolution results
        canonical_product_id=sku,
        confidence_score=confidence,
        matched_by=matched_by,
        # Optional enrichment
        brand=brand,
        variant=None,  # reserved for future extraction
    )
