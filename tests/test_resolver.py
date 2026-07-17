"""
Tests for src/entities/resolver.py

Covers the full resolution pipeline: strategy priority, result structure,
batch processing, gold set evaluation, and unmatched listing handling.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.domain.models import Condition, Currency, RawListing, ResolvedListing, Source
from src.entities.catalog import load_catalog
from src.entities.resolver import resolve, resolve_batch

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


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
        timestamp=datetime.now(UTC),
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

    def test_variant_extracted(self, catalog):
        listing = make_listing("GPU ASUS TUF RTX 4070 OC 12GB")
        result = resolve(listing, catalog)
        assert result.variant == "Tuf"

    def test_variant_none_when_unknown(self, catalog):
        listing = make_listing("GPU ASUS RTX 4070")
        result = resolve(listing, catalog)
        assert result.variant is None

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


# ---------------------------------------------------------------------------
# Gold set evaluation
# ---------------------------------------------------------------------------


class TestGoldSet:
    """Measures precision and recall against a labeled gold set.

    The gold set is a JSON file with 30+ real-world listing titles
    and their expected canonical SKUs. Precision and recall are computed
    against the resolver output.

    Thresholds:
        precision >= 0.95
        recall    >= 0.90
    """

    @pytest.fixture(scope="class")
    def gold_set(self) -> list[dict]:
        path = FIXTURES_DIR / "gold_set.json"
        if not path.exists():
            pytest.skip("Gold set not found — skipping end-to-end evaluation")
        return json.loads(path.read_text(encoding="utf-8"))

    @pytest.fixture(scope="class")
    def full_catalog(self) -> dict:
        return load_catalog()

    def test_gold_set_precision_and_recall(self, gold_set, full_catalog):
        true_positives = 0
        false_positives = 0
        false_negatives = 0

        for entry in gold_set:
            title = entry["title"]
            expected = entry["expected_sku"]

            listing = RawListing(
                source=Source.THOT,
                url=f"https://test.uy/{title.replace(' ', '-').lower()}",
                timestamp=datetime.now(UTC),
                title=title,
                price=500.0,
                currency=Currency.USD,
                seller="test",
                condition=Condition.NEW,
            )

            result = resolve(listing, full_catalog)

            if expected and result.canonical_product_id == expected:
                true_positives += 1
            elif expected and result.canonical_product_id != expected:
                if result.canonical_product_id is not None:
                    false_positives += 1
                else:
                    false_negatives += 1
            elif not expected and result.canonical_product_id is not None:
                false_positives += 1

        total_positive = sum(1 for e in gold_set if e["expected_sku"])
        predicted_positive = true_positives + false_positives

        precision = true_positives / predicted_positive if predicted_positive > 0 else 0.0
        recall = true_positives / total_positive if total_positive > 0 else 0.0

        assert precision >= 0.95, (
            f"Precision {precision:.3f} < 0.95 ({true_positives}/{predicted_positive})"
        )
        assert recall >= 0.90, f"Recall {recall:.3f} < 0.90 ({true_positives}/{total_positive})"
