"""
Manufacturer part number (MPN) extraction.

Some stores (mostly Banifox and PCCompu) append the manufacturer part number
to the title. Two listings with the same MPN are the same product even when
their titles describe it differently. Only brand-specific, unambiguous
patterns are recognised; the result is normalized (uppercase, no separators).
"""

from __future__ import annotations

import re

_PATTERNS = [
    re.compile(r"\bGV-[NR][0-9A-Z]{3,}(?:[ -][0-9A-Z]{1,6}){0,2}-\d{1,2}G[DB]?\b"),  # Gigabyte GPU
    re.compile(r"\b912-V\d{3}-\d{3}\b"),  # MSI GPU
    re.compile(
        r"\b(?:DUAL|TUF|PRIME|ROG-STRIX|ROG-ASTRAL)-(?:RTX|RX|GTX|GT)[0-9A-Z]+-[0-9A-Z-]+\b"
    ),
    re.compile(r"\b90YV0[0-9A-Z]{3}-M0[0-9A-Z]{4}\b"),  # ASUS part number
    re.compile(r"\bKF\d{3}[CR]\d{2}[0-9A-Z]+(?:-\d{1,3})?\b"),  # Kingston Fury
    re.compile(r"\bKVR\d{2}[A-Z]\d{2}[0-9A-Z]+(?:/\d{1,3})?\b"),  # Kingston ValueRAM
    re.compile(r"\bS(?:NV3S|NVS|A400S37|KC3000[DS]|FYR[DS])/\d{3,4}G\b"),  # Kingston SSD
    re.compile(r"\bCM[KHTSW][0-9A-Z]{8,}\b"),  # Corsair RAM
    re.compile(r"\bCSSD-F\d{4}GB[0-9A-Z]+\b"),  # Corsair SSD
    re.compile(r"\bP(?:SD|SP|VE|VER|VV|VVR|VX|VXR|VS|VSR)\d[0-9A-Z]{6,}\b"),  # Patriot RAM
    re.compile(r"\bNT[0-9A-Z]{3,}-[0-9A-Z]{2,4}(?:-[0-9A-Z]{2,4})?\b"),  # Netac
    re.compile(r"\bAX5U\d{4}C\d{2,4}G-[A-Z]+\b"),  # ADATA XPG RAM
    re.compile(r"\bAD[45][US]\d{6}G\d{2}-[A-Z]+\b"),  # ADATA RAM
    re.compile(r"\bHSC\d{3}[US]\d{2}[A-Z0-9]{2}\b"),  # Hiksemi RAM
]


def normalize_mpn(value: str) -> str:
    return re.sub(r"[^0-9A-Z]", "", value.upper())


def extract_mpn(title: str) -> str | None:
    upper = title.upper()
    for pattern in _PATTERNS:
        match = pattern.search(upper)
        if match:
            mpn = normalize_mpn(match.group(0))
            if len(mpn) >= 8 and any(c.isdigit() for c in mpn) and any(c.isalpha() for c in mpn):
                return mpn
    return None
