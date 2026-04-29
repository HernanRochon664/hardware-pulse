"""
Tests for src/entities/catalog.py

Covers catalog loading, validation, and query helpers.
"""

import pytest
import yaml
from pathlib import Path

from src.entities.catalog import load_catalog, get_all_skus, get_skus_by_brand


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_catalog_file(tmp_path: Path) -> Path:
    """Write a minimal valid catalog YAML to a temp file."""
    data = {
        "canonical_products": [
            {"sku": "RTX 4070", "brand_family": "NVIDIA", "release_year": 2023},
            {"sku": "RTX 4070 Ti", "brand_family": "NVIDIA", "release_year": 2023},
            {"sku": "RX 7800 XT", "brand_family": "AMD", "release_year": 2023},
            {"sku": "Arc B580", "brand_family": "Intel", "release_year": 2024},
        ]
    }
    path = tmp_path / "catalog.yaml"
    path.write_text(yaml.dump(data), encoding="utf-8")
    return path


@pytest.fixture
def minimal_catalog(minimal_catalog_file: Path) -> dict:
    return load_catalog(minimal_catalog_file)


# ---------------------------------------------------------------------------
# Loading and structure
# ---------------------------------------------------------------------------


class TestCatalogLoading:
    def test_loads_successfully(self, minimal_catalog):
        assert len(minimal_catalog) == 4

    def test_skus_are_keys(self, minimal_catalog):
        assert "RTX 4070" in minimal_catalog
        assert "RX 7800 XT" in minimal_catalog

    def test_metadata_stored_correctly(self, minimal_catalog):
        entry = minimal_catalog["RTX 4070"]
        assert entry["brand_family"] == "NVIDIA"
        assert entry["release_year"] == 2023

    def test_sku_not_in_metadata(self, minimal_catalog):
        """The 'sku' key should not appear inside the metadata dict."""
        for meta in minimal_catalog.values():
            assert "sku" not in meta

    def test_file_not_found_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_catalog(tmp_path / "nonexistent.yaml")

    def test_missing_canonical_products_key_raises(self, tmp_path: Path):
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump({"wrong_key": []}), encoding="utf-8")
        with pytest.raises(ValueError, match="canonical_products"):
            load_catalog(path)

    def test_duplicate_sku_raises(self, tmp_path: Path):
        data = {
            "canonical_products": [
                {"sku": "RTX 4070", "brand_family": "NVIDIA"},
                {"sku": "RTX 4070", "brand_family": "NVIDIA"},
            ]
        }
        path = tmp_path / "dup.yaml"
        path.write_text(yaml.dump(data), encoding="utf-8")
        with pytest.raises(ValueError, match="Duplicate"):
            load_catalog(path)

    def test_entry_missing_sku_raises(self, tmp_path: Path):
        data = {"canonical_products": [{"brand_family": "NVIDIA"}]}
        path = tmp_path / "nosku.yaml"
        path.write_text(yaml.dump(data), encoding="utf-8")
        with pytest.raises(ValueError, match="sku"):
            load_catalog(path)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


class TestCatalogHelpers:
    def test_get_all_skus(self, minimal_catalog):
        skus = get_all_skus(minimal_catalog)
        assert "RTX 4070" in skus
        assert "RX 7800 XT" in skus
        assert len(skus) == 4

    def test_get_skus_by_brand_nvidia(self, minimal_catalog):
        nvidia = get_skus_by_brand(minimal_catalog, "NVIDIA")
        assert "RTX 4070" in nvidia
        assert "RTX 4070 Ti" in nvidia
        assert "RX 7800 XT" not in nvidia

    def test_get_skus_by_brand_case_insensitive(self, minimal_catalog):
        assert get_skus_by_brand(minimal_catalog, "nvidia") == \
               get_skus_by_brand(minimal_catalog, "NVIDIA")

    def test_get_skus_by_brand_unknown(self, minimal_catalog):
        assert get_skus_by_brand(minimal_catalog, "Unknown") == []