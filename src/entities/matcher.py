"""
SKU matching strategies for hardware-pulse entity resolution.

Responsibilities:
- Match normalized titles against the canonical catalog
- Return (sku, confidence_score) tuples
- Apply strategies in order: exact → regex → fuzzy

Does NOT:
- Normalize titles (see normalizer.py)
- Load the catalog (see catalog.py)
- Orchestrate the full resolution pipeline (see resolver.py)
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

from src.entities.normalizer import normalize_sku, normalize_title

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

MatchResult = tuple[str | None, float]  # (canonical_sku | None, confidence)
Catalog = dict[str, dict[str, Any]]

# ---------------------------------------------------------------------------
# Confidence scores
# ---------------------------------------------------------------------------

EXACT_CONFIDENCE = 1.0
REGEX_CONFIDENCE = 0.9
FUZZY_MIN_THRESHOLD = 0.8

# ---------------------------------------------------------------------------
# Exact match
# ---------------------------------------------------------------------------


def exact_match(title: str, catalog: Catalog) -> MatchResult:
    """
    Match a title against the catalog after normalizing both sides.

    Strategy:
    - Normalize the title
    - For each catalog SKU, check if normalize_sku(sku) appears
        as a substring in the normalized title

    We check substring containment, not equality, because titles
    contain brand/variant noise around the SKU.
    e.g. "gpu msi geforce rtx 3050 ventus x2 oc" contains "rtx 3050"

    Args:
        title:   Raw product title.
        catalog: Loaded canonical catalog.

    Returns:
        (sku, 1.0) if exact match found, (None, 0.0) otherwise.
    """
    normalized = normalize_title(title)

    # Check longer SKUs first to avoid "RTX 4070" matching before "RTX 4070 Ti"
    skus_by_length = sorted(catalog.keys(), key=len, reverse=True)

    for sku in skus_by_length:
        normalized_sku = normalize_sku(sku)
        if normalized_sku in normalized:
            return sku, EXACT_CONFIDENCE

    return None, 0.0


# ---------------------------------------------------------------------------
# Regex match
# ---------------------------------------------------------------------------

# Pre-compiled patterns for common GPU model formats
_GPU_PATTERNS: list[re.Pattern] = [
    # NVIDIA RTX with optional suffix
    re.compile(r"\bRTX\s*(\d{4})\s*(Ti|Super|Ultra)?\b", re.IGNORECASE),
    # AMD RX with optional XT suffix, also catches compact "R9070XT"
    re.compile(r"\bR[Xx]?\s*(\d{4})\s*(XT)?\b", re.IGNORECASE),
    # AMD Radeon without RX prefix (e.g., "radeon 7600" → "RX 7600")
    re.compile(r"\bradeon\s*(\d{4})\b", re.IGNORECASE),
    # NVIDIA GeForce without RTX prefix (e.g., "geforce 5090" → "RTX 5090")
    re.compile(r"\bgeforce\s*(\d{4})\b", re.IGNORECASE),
    # GIGABYTE product codes (e.g., "GV-R906X" → "RX 9060 XT")
    re.compile(r"\bGV-R(\d{3,4})X", re.IGNORECASE),
    # Intel Arc
    re.compile(r"\bArc\s*([AB]\d{3})\b", re.IGNORECASE),
    # GTX legacy
    re.compile(r"\bGTX\s*(\d{3,4})\b", re.IGNORECASE),
]


def _reconstruct_sku_from_match(match: re.Match) -> str:
    """
    Reconstruct a normalized SKU string from a regex match.

    Handles NVIDIA RTX, AMD RX, AMD Radeon (without RX), Intel Arc, and GTX patterns.
    """
    full = match.group(0).upper().strip()

    # Normalize internal whitespace
    full = re.sub(r"\s+", " ", full)

    # Fix "R9070" → "RX 9070"
    full = re.sub(r"^R(\d)", r"RX \1", full)

    # Fix "RADEON 7600" → "RX 7600"
    full = re.sub(r"^RADEON\s+(\d{4})$", r"RX \1", full)

    # Fix "GEFORCE 5090" → "RTX 5090"
    full = re.sub(r"^GEFORCE\s+(\d{4})$", r"RTX \1", full)

    # Fix "GV-R906X" → "RX 9060 XT", "GV-R907X" → "RX 9070 XT"
    if re.match(r"^GV-R\d{3,4}X$", full):
        code = re.sub(r"GV-R(\d{3,4})X", r"\1", full)
        if code == "906":
            full = "RX 9060 XT"
        elif code == "907":
            full = "RX 9070 XT"

    # Ensure space between prefix and number
    full = re.sub(r"(RTX|RX|GTX|Arc)(\d)", r"\1 \2", full)

    # Ensure space before suffix
    full = re.sub(r"(\d)(Ti|XT|Super|Ultra)$", r"\1 \2", full, flags=re.IGNORECASE)

    return full.strip()


def regex_match(title: str, catalog: Catalog) -> MatchResult:
    """
    Extract GPU model using regex patterns and validate against catalog.

    Strategy:
    1. Apply each GPU pattern to the title
    2. Reconstruct candidate SKU from match
    3. Check if candidate matches any catalog SKU (normalized)

    Args:
        title:   Raw product title.
        catalog: Loaded canonical catalog.

    Returns:
        (sku, 0.9) if regex match found in catalog, (None, 0.0) otherwise.
    """
    for pattern in _GPU_PATTERNS:
        match = pattern.search(title)
        if not match:
            continue

        candidate = _reconstruct_sku_from_match(match)
        candidate_normalized = normalize_sku(candidate)

        # Validate candidate against catalog
        skus_by_length = sorted(catalog.keys(), key=len, reverse=True)
        for sku in skus_by_length:
            if normalize_sku(sku) == candidate_normalized:
                return sku, REGEX_CONFIDENCE

    return None, 0.0


# ---------------------------------------------------------------------------
# Fuzzy match
# ---------------------------------------------------------------------------


def fuzzy_match(
    title: str,
    catalog: Catalog,
    threshold: float = FUZZY_MIN_THRESHOLD,
) -> MatchResult:
    """
    Fuzzy fallback using sequence similarity against catalog SKUs.

    Strategy:
    - Normalize the title
    - For each catalog SKU, compute similarity ratio between
        normalize_sku(sku) and the normalized title (substring search)
    - Return best match if score >= threshold

    We use SequenceMatcher from difflib (stdlib) rather than
    rapidfuzz here for simplicity. rapidfuzz can be swapped in
    later for better performance at scale.

    Args:
        title:     Raw product title.
        catalog:   Loaded canonical catalog.
        threshold: Minimum similarity score (0.0–1.0) to accept.

    Returns:
        (sku, score) if best score >= threshold, (None, 0.0) otherwise.
    """
    normalized = normalize_title(title)
    best_sku: str | None = None
    best_score: float = 0.0

    for sku in catalog:
        normalized_sku = normalize_sku(sku)

        # Use SequenceMatcher to find similarity
        # We compare the SKU against substrings of the title
        # to handle titles with extra noise around the model name
        ratio = SequenceMatcher(None, normalized_sku, normalized).ratio()

        if ratio > best_score:
            best_score = ratio
            best_sku = sku

    if best_score >= threshold:
        return best_sku, best_score

    return None, 0.0
