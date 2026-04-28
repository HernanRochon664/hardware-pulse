"""
Entity resolution module for hardware-pulse.

This module provides functionality for resolving product listings to canonical
GPU SKUs through various matching strategies (exact, regex, fuzzy).
"""

# Types
from .catalog import Catalog, CatalogEntry
from .matcher import MatchResult

# Catalog management
from .catalog import get_all_skus, get_skus_by_brand, load_catalog

# Text normalization
from .normalizer import extract_brand, normalize_sku, normalize_title

# Matching strategies
from .matcher import exact_match, fuzzy_match, regex_match

# Resolution pipeline
from .resolver import resolve, resolve_batch

__all__ = [
    # Types
    "Catalog",
    "CatalogEntry",
    "MatchResult",
    # Catalog management
    "load_catalog",
    "get_all_skus",
    "get_skus_by_brand",
    # Text normalization
    "normalize_title",
    "normalize_sku",
    "extract_brand",
    # Matching strategies
    "exact_match",
    "regex_match",
    "fuzzy_match",
    # Resolution pipeline
    "resolve",
    "resolve_batch",
]
