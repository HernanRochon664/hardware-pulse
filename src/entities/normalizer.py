"""
Title normalizer for hardware-pulse entity resolution.

Responsibilities:
- Clean and standardize raw product titles for matching
- Normalize GPU model tokens to canonical form
- Extract brand information from titles

Does NOT:
- Perform matching against the catalog
- Access the database
- Modify original listings
"""

from __future__ import annotations

import re
import unicodedata


# ---------------------------------------------------------------------------
# Noise patterns
# ---------------------------------------------------------------------------

# Separators to replace with spaces
_SEPARATOR_PATTERN = re.compile(r"[-_/\\.,()|\[\]{}+:;]+")

# Multiple spaces collapser
_WHITESPACE_PATTERN = re.compile(r"\s+")

# Generic noise words that don't contribute to SKU identification
# OC is intentionally excluded, it's part of variant, not noise
_NOISE_WORDS = {
    "gpu", "tarjeta", "video", "placa", "de",
    "pcie", "pci", "express",
    "gddr6x", "gddr6", "gddr5x", "gddr5", "gddr4", "ddr5", "ddr4", "ddr3",
    "gb", "tb", "bit", "bits",
    "hdmi", "displayport", "dport", "vga", "dvi",
    "gaming", "edition", "series",
}

# ---------------------------------------------------------------------------
# SKU token normalization patterns
# ---------------------------------------------------------------------------

# These patterns normalize compact model strings to spaced canonical form
# Order matters — more specific patterns first
_SKU_NORMALIZATIONS: list[tuple[re.Pattern, str]] = [
    # NVIDIA RTX series — handle Ti/Super/Ultra suffixes
    (re.compile(r"\bRTX\s*(\d{4})\s*Ti\b", re.IGNORECASE),   r"RTX \1 Ti"),
    (re.compile(r"\bRTX\s*(\d{4})\s*Super\b", re.IGNORECASE), r"RTX \1 Super"),
    (re.compile(r"\bRTX\s*(\d{4})\b", re.IGNORECASE),         r"RTX \1"),

    # AMD RX series — handle XT suffix and compact forms
    # "R9070XT" → "RX 9070 XT" (missing RX prefix)
    (re.compile(r"\bR\s*(\d{4})\s*XT\b", re.IGNORECASE),      r"RX \1 XT"),
    (re.compile(r"\bRX\s*(\d{4})\s*XT\b", re.IGNORECASE),     r"RX \1 XT"),
    (re.compile(r"\bRX\s*(\d{4})\b", re.IGNORECASE),          r"RX \1"),

    # Intel Arc series
    (re.compile(r"\bArc\s*([AB]\d{3})\b", re.IGNORECASE),     r"Arc \1"),

    # GTX legacy
    (re.compile(r"\bGTX\s*(\d{3,4})\b", re.IGNORECASE),       r"GTX \1"),

    # GT legacy (e.g. G210, GT710)
    (re.compile(r"\bG\s*(\d{3})\b", re.IGNORECASE),           r"GT \1"),
    (re.compile(r"\bGT\s*(\d{3})\b", re.IGNORECASE),          r"GT \1"),
]

# ---------------------------------------------------------------------------
# Brand dictionaries
# ---------------------------------------------------------------------------

# ④ Brand detection uses token matching — longest match wins
_BRAND_TOKENS: dict[str, list[str]] = {
    "ASUS":     ["asus", "rog", "tuf", "dual", "prime"],
    "MSI":      ["msi"],
    "Gigabyte": ["gigabyte", "aorus", "windforce", "eagle"],
    "Palit":    ["palit", "stormx", "gamerock", "gamingpro"],
    "Zotac":    ["zotac", "twin", "amp"],
    "ASRock":   ["asrock"],
    "XFX":      ["xfx", "swift", "speedster"],
    "Biostar":  ["biostar"],
    "Sapphire": ["sapphire", "pulse", "nitro"],
    "PowerColor": ["powercolor", "hellhound", "fighter"],
    "PNY":      ["pny"],
    "Arktek":   ["arktek"],
}


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def normalize_title(title: str) -> str:
    """
    Normalize a raw product title for SKU matching.

    Pipeline:
    1. Strip leading/trailing whitespace
    2. Unicode normalization (remove accents)
    3. Replace separators with spaces
    4. Apply SKU token normalization (RTX4070 → RTX 4070)
    5. Remove generic noise words
    6. Collapse multiple spaces
    7. Lowercase for consistent comparison

    Args:
        title: Raw product title from scraper.

    Returns:
        Cleaned, normalized title string.

    Example:
        "GIGABYTE AMD RADEON R9070XT GV-R907XGAMINGOCICE-16GD"
        → "gigabyte amd radeon rx 9070 xt gv r907xgamingocice 16gd"
    """
    if not title:
        return ""

    # Step 1: Strip
    text = title.strip()

    # Step 2: Unicode normalization, remove accents
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))

    # Step 3: Replace separators with spaces
    text = _SEPARATOR_PATTERN.sub(" ", text)

    # Step 4: Normalize SKU tokens (before lowercasing, patterns are case-insensitive)
    for pattern, replacement in _SKU_NORMALIZATIONS:
        text = pattern.sub(replacement, text)

    # Step 5: Lowercase
    text = text.lower()

    # Step 6: Remove noise words (whole tokens only)
    tokens = text.split()
    tokens = [t for t in tokens if t not in _NOISE_WORDS]
    text = " ".join(tokens)

    # Step 7: Collapse whitespace
    text = _WHITESPACE_PATTERN.sub(" ", text).strip()

    return text


def normalize_sku(sku: str) -> str:
    """
    Normalize a catalog SKU for comparison.

    Applies the same whitespace/case normalization as normalize_title
    but skips noise word removal (SKUs are already clean).

    Args:
        sku: Canonical SKU string (e.g. "RTX 4070", "RX 9070 XT").

    Returns:
        Lowercased, whitespace-normalized SKU.
    """
    text = sku.strip().lower()
    text = _WHITESPACE_PATTERN.sub(" ", text)
    return text


def extract_brand(title: str) -> str | None:
    """
    Extract AIB brand from a product title.

    Uses token matching — returns the first brand whose tokens
    appear in the lowercased title.

    Args:
        title: Raw or normalized product title.

    Returns:
        Brand name string (e.g. "ASUS", "MSI") or None if not detected.

    Longest match is implicitly handled by dict ordering,
    more specific brands (MSI) are checked before generic ones.
    """
    lower = title.lower()
    for brand, tokens in _BRAND_TOKENS.items():
        if any(token in lower for token in tokens):
            return brand
    return None