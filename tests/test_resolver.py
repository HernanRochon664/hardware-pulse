"""
Tests for src/entities/resolver.py

Covers the full resolution pipeline: strategy priority, result structure,
batch processing, and unmatched listing handling.
"""

from datetime import datetime, timezone

import pytest

from src.domain.models import Condition, Currency, RawListing, ResolvedListing, Source
from src.entities.resolver import resolve, resolve_batch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog() -> dict:
    return {
        "RTX 4070": {"brand_family": "NVIDIA", "release_year": 2023},
        "RTX 4070 Ti": {"brand_family": "NVIDIA", "release_year": 2023},
        "RTX 3050": {"brand_family": "NVIDIA", "release_year": 2021},
        "RX 9070 XT": {"brand_family": "AMD", "release_year": 2025},
        "Arc B580": {"brand_family": "Intel", "release_year": 2024},
    }


def make_listing(title: str, price: float = 500.0) -> RawListing:
    return RawListing(
        source=Source.THOT,
        url=f"https://thot.uy/{title.replace(' ', '-').lower()}",
        timestamp=datetime.now(timezone.utc),
        title=title,
        price=price,
        currency=Currency.USD,
        seller="thot",
        condition=Condition.NEW,
    )


# ---------------------------------------------------------------------------
# resolve() - single listing
# ---------------------------------------------------------------------------


class TestResolve:
    def test_returns_resolved_listing(self, catalog):
        listing = make_listing("GPU MSI GeForce RTX 3050 Ventus OC")
        result = resolve(listing, catalog)
        assert isinstance(result, ResolvedListing)

    def test_matched_listing_has_canonical_id(self, catalog):
        listing = make_listing("GPU MSI GeForce RTX 3050 Ventus OC")
        result = resolve(listing, catalog)
        assert result.canonical_product_id == "RTX 3050"

    def test_unmatched_listing_has_none_canonical_id(self, catalog):
        listing = make_listing("Pasta Termica Deep Cool Z3", price=10.0)
        result = resolve(listing, catalog)
        assert result.canonical_product_id is None

    def test_matched_by_exact_has_confidence_1(self, catalog):
        listing = make_listing("GPU ASUS TUF RTX 4070 OC 12GB")
        result = resolve(listing, catalog)
        assert result.matched_by == "exact"
        assert result.confidence_score == 1.0

    def test_unmatched_has_zero_confidence(self, catalog):
        listing = make_listing("Riser Cable PCIe 4.0", price=20.0)
        result = resolve(listing, catalog)
        assert result.confidence_score == 0.0
        assert result.matched_by is None

    def test_traceability_fields_preserved(self, catalog):
        listing = make_listing("GPU MSI GeForce RTX 3050 Ventus OC")
        result = resolve(listing, catalog)
        assert result.title == listing.title
        assert result.price == listing.price
        assert result.source == listing.source
        assert result.seller == listing.seller

    def test_brand_extracted(self, catalog):
        listing = make_listing("GPU MSI GeForce RTX 3050 Ventus OC")
        result = resolve(listing, catalog)
        assert result.brand == "MSI"

    def test_brand_none_for_unknown(self, catalog):
        listing = make_listing("GPU UnknownBrand RTX 3050")
        result = resolve(listing, catalog)
        assert result.brand is None

    # Strategy priority
    def test_exact_takes_priority_over_regex(self, catalog):
        """When exact match succeeds, matched_by should be 'exact'."""
        listing = make_listing("ASUS TUF RTX 4070 Ti OC 12GB")
        result = resolve(listing, catalog)
        assert result.matched_by == "exact"
        assert result.canonical_product_id == "RTX 4070 Ti"

    def test_ti_suffix_resolved_correctly(self, catalog):
        """RTX 4070 Ti should not collapse to RTX 4070."""
        listing = make_listing("ASUS TUF RTX 4070 Ti OC 12GB")
        result = resolve(listing, catalog)
        assert result.canonical_product_id == "RTX 4070 Ti"

    def test_compact_amd_format_resolves(self, catalog):
        """R9070XT (Banifox format) should resolve to RX 9070 XT."""
        listing = make_listing("GIGABYTE AMD RADEON R9070XT GV-R907XGAMINGOCICE-16GD")
        result = resolve(listing, catalog)
        assert result.canonical_product_id == "RX 9070 XT"


# ---------------------------------------------------------------------------
# resolve_batch()
# ---------------------------------------------------------------------------


class TestResolveBatch:
    def test_returns_list_of_resolved_listings(self, catalog):
        listings = [
            make_listing("GPU MSI RTX 3050 Ventus OC"),
            make_listing("Pasta Termica", price=8.0),
        ]
        results = resolve_batch(listings, catalog)
        assert len(results) == 2
        assert all(isinstance(r, ResolvedListing) for r in results)

    def test_preserves_order(self, catalog):
        listings = [
            make_listing("GPU MSI RTX 3050 Ventus OC"),
            make_listing("ASUS TUF RTX 4070 Ti OC"),
            make_listing("Pasta Termica", price=8.0),
        ]
        results = resolve_batch(listings, catalog)
        assert results[0].canonical_product_id == "RTX 3050"
        assert results[1].canonical_product_id == "RTX 4070 Ti"
        assert results[2].canonical_product_id is None

    def test_empty_list_returns_empty(self, catalog):
        results = resolve_batch([], catalog)
        assert results == []

    def test_all_matched_count(self, catalog):
        listings = [
            make_listing("GPU MSI RTX 3050 Ventus OC"),
            make_listing("ASUS TUF RTX 4070 OC"),
        ]
        results = resolve_batch(listings, catalog)
        matched = sum(1 for r in results if r.canonical_product_id is not None)
        assert matched == 2

    def test_mixed_match_unmatched(self, catalog):
        listings = [
            make_listing("GPU MSI RTX 3050"),
            make_listing("Pasta Termica", price=8.0),
            make_listing("Riser Cable", price=15.0),
        ]
        results = resolve_batch(listings, catalog)
        matched = [r for r in results if r.canonical_product_id is not None]
        unmatched = [r for r in results if r.canonical_product_id is None]
        assert len(matched) == 1
        assert len(unmatched) == 2