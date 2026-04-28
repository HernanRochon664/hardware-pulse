"""
Canonical product catalog loader for hardware-pulse.

Responsibilities:
- Load and parse configs/catalog.yaml
- Return a typed dict for O(1) SKU lookup
- Validate catalog structure on load

Does NOT:
- Perform matching or normalization
- Access the database
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

CatalogEntry = dict[str, Any]
Catalog = dict[str, CatalogEntry]

# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

DEFAULT_CATALOG_PATH = Path(__file__).parent.parent.parent / "configs" / "catalog.yaml"


def load_catalog(path: Path | None = None) -> Catalog:
    """
    Load the canonical GPU catalog from YAML.

    Args:
        path: Path to catalog.yaml. Defaults to configs/catalog.yaml
            relative to project root.

    Returns:
        Dict mapping canonical SKU → metadata dict.
        Example:
            {
                "RTX 4070": {"brand_family": "NVIDIA", "release_year": 2023},
                "RX 9070 XT": {"brand_family": "AMD", "release_year": 2025},
            }

    Raises:
        FileNotFoundError: If catalog file does not exist.
        ValueError: If catalog structure is invalid.

    We resolve path relative to this file, not cwd, so the loader
    works regardless of where the script is executed from.
    """
    if path is None:
        path = DEFAULT_CATALOG_PATH

    if not path.exists():
        raise FileNotFoundError(f"Catalog file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))

    # Validate top-level structure before processing
    if not isinstance(raw, dict) or "canonical_products" not in raw:
        raise ValueError(
            f"Invalid catalog format: expected top-level 'canonical_products' key in {path}"
        )

    entries = raw["canonical_products"]
    if not isinstance(entries, list):
        raise ValueError("'canonical_products' must be a list of entries")

    catalog: Catalog = {}

    for entry in entries:
        if not isinstance(entry, dict) or "sku" not in entry:
            raise ValueError(f"Invalid catalog entry (missing 'sku'): {entry}")

        sku: str = entry["sku"]

        if sku in catalog:
            raise ValueError(f"Duplicate SKU in catalog: '{sku}'")

        # Store all fields except 'sku' as metadata
        metadata: CatalogEntry = {k: v for k, v in entry.items() if k != "sku"}
        catalog[sku] = metadata

    return catalog


def get_all_skus(catalog: Catalog) -> list[str]:
    """Return all canonical SKUs in the catalog."""
    return list(catalog.keys())


def get_skus_by_brand(catalog: Catalog, brand_family: str) -> list[str]:
    """
    Return SKUs filtered by brand family.

    Args:
        catalog:      Loaded catalog dict.
        brand_family: e.g. "NVIDIA", "AMD", "Intel"

    Returns:
        List of SKUs belonging to that brand family.
    """
    return [
        sku for sku, meta in catalog.items()
        if meta.get("brand_family", "").upper() == brand_family.upper()
    ]