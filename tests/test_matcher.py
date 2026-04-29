"""
Tests for src/entities/matcher.py

Covers the three critical failure modes identified in design:
1. SKU suffix ordering (RTX 4070 Ti vs RTX 4070)
2. SKU reconstruction from regex matches
3. Normalization consistency across strategies
"""

import pytest

from src.entities.matcher import exact_match, fuzzy_match, regex_match


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog() -> dict:
    """Minimal catalog covering the cases tested below."""
    return {
        "RTX 4070": {"brand_family": "NVIDIA", "release_year": 2023},
        "RTX 4070 Ti": {"brand_family": "NVIDIA", "release_year": 2023},
        "RTX 3050": {"brand_family": "NVIDIA", "release_year": 2021},
        "RTX 5090": {"brand_family": "NVIDIA", "release_year": 2025},
        "RX 9070 XT": {"brand_family": "AMD", "release_year": 2025},
        "RX 9070": {"brand_family": "AMD", "release_year": 2025},
        "RX 9060 XT": {"brand_family": "AMD", "release_year": 2025},
        "RX 7600": {"brand_family": "AMD", "release_year": 2023},
        "Arc B580": {"brand_family": "Intel", "release_year": 2024},
    }


# ---------------------------------------------------------------------------
# exact_match
# ---------------------------------------------------------------------------


class TestExactMatch:
    def test_basic_match(self, catalog):
        sku, score = exact_match("GPU MSI GeForce RTX 3050 Ventus OC", catalog)
        assert sku == "RTX 3050"
        assert score == 1.0

    def test_no_match_returns_none(self, catalog):
        sku, score = exact_match("Pasta Termica Deep Cool Z3", catalog)
        assert sku is None
        assert score == 0.0

    def test_accessory_with_model_in_name_no_match(self, catalog):
        """Riser cable with RTX40 in name should NOT match any catalog SKU."""
        sku, score = exact_match(
            "Antec Riser Cable Vertical GPU AT-RCVB-BK200-PCIE4-RTX40", catalog
        )
        assert sku is None

    # ① CRITICAL: suffix ordering
    def test_ti_matched_before_base_model(self, catalog):
        """RTX 4070 Ti title must match 'RTX 4070 Ti', not 'RTX 4070'."""
        sku, score = exact_match("ASUS TUF RTX 4070 Ti OC 12GB", catalog)
        assert sku == "RTX 4070 Ti"
        assert score == 1.0

    def test_base_model_matched_when_no_suffix(self, catalog):
        sku, score = exact_match("MSI RTX 4070 Gaming X Trio", catalog)
        assert sku == "RTX 4070"

    def test_rx_compact_format_matches(self, catalog):
        """R9070XT (Banifox format) should match 'RX 9070 XT'."""
        sku, score = exact_match(
            "GIGABYTE AMD RADEON R9070XT GV-R907XGAMINGOCICE-16GD", catalog
        )
        assert sku == "RX 9070 XT"

    def test_arc_matches(self, catalog):
        sku, score = exact_match(
            "Tarjeta de Video ASRock Intel ARC B580 Challenger OC", catalog
        )
        assert sku == "Arc B580"

    def test_returns_tuple(self, catalog):
        result = exact_match("RTX 3050", catalog)
        assert isinstance(result, tuple)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# regex_match
# ---------------------------------------------------------------------------


class TestRegexMatch:
    def test_basic_nvidia_match(self, catalog):
        sku, score = regex_match("GPU MSI GeForce RTX 3050 Ventus OC", catalog)
        assert sku == "RTX 3050"
        assert score == 0.9

    def test_amd_without_rx_prefix(self, catalog):
        """'radeon 7600' without RX prefix should match 'RX 7600'."""
        sku, score = regex_match("GIGABYTE AMD RADEON 7600 GAMING OC", catalog)
        assert sku == "RX 7600"

    def test_compact_rtx_no_space(self, catalog):
        sku, score = regex_match("RTX3050 Stormx 6GB", catalog)
        assert sku == "RTX 3050"

    def test_no_match_for_accessory(self, catalog):
        sku, score = regex_match("Pasta Termica Deep Cool Z3", catalog)
        assert sku is None
        assert score == 0.0

    def test_score_is_0_9(self, catalog):
        _, score = regex_match("RTX 3050 Gaming", catalog)
        assert score == 0.9

    def test_intel_arc_match(self, catalog):
        sku, score = regex_match("ASRock Arc B580 Challenger OC 12GB", catalog)
        assert sku == "Arc B580"

    # Normalization consistency
    def test_regex_and_exact_agree_on_same_title(self, catalog):
        title = "GPU MSI GeForce RTX 3050 Ventus OC"
        sku_exact, _ = exact_match(title, catalog)
        sku_regex, _ = regex_match(title, catalog)
        assert sku_exact == sku_regex


# ---------------------------------------------------------------------------
# fuzzy_match
# ---------------------------------------------------------------------------


class TestFuzzyMatch:
    def test_matches_above_threshold(self, catalog):
        sku, score = fuzzy_match("RTX 3050 GPU", catalog, threshold=0.5)
        assert sku is not None
        assert score >= 0.5

    def test_returns_none_below_threshold(self, catalog):
        sku, score = fuzzy_match("Pasta Termica", catalog, threshold=0.9)
        assert sku is None
        assert score == 0.0

    def test_score_between_0_and_1(self, catalog):
        _, score = fuzzy_match("RTX 3050", catalog)
        assert 0.0 <= score <= 1.0

    def test_returns_tuple(self, catalog):
        result = fuzzy_match("RTX 3050", catalog)
        assert isinstance(result, tuple)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Cross-strategy consistency
# ---------------------------------------------------------------------------


class TestCrossStrategyConsistency:
    """
    CRITICAL: all strategies must agree when the signal is unambiguous.
    If exact and regex disagree on a clear title, normalization is broken.
    """

    def test_all_strategies_agree_on_clear_title(self, catalog):
        title = "GPU MSI GeForce RTX 3050 Ventus X2 OC"
        sku_exact, _ = exact_match(title, catalog)
        sku_regex, _ = regex_match(title, catalog)
        assert sku_exact == sku_regex == "RTX 3050"

    def test_ti_suffix_consistent_across_strategies(self, catalog):
        title = "ASUS TUF RTX 4070 Ti OC 12GB"
        sku_exact, _ = exact_match(title, catalog)
        sku_regex, _ = regex_match(title, catalog)
        assert sku_exact == "RTX 4070 Ti"
        assert sku_regex == "RTX 4070 Ti"