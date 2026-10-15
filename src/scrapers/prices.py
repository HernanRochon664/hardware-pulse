"""
Price and currency parsing for store-formatted strings.

Uruguayan stores format prices as "U$S 1.049" or "USD 1.049,50" (dot for
thousands, comma for decimals) while WooCommerce themes may use "US$1,049.00".
The separators are explicit per adapter so "1.049" is never read as 1.049.
"""

from __future__ import annotations

import re

from src.domain.models import Currency

_NUMBER = re.compile(r"\d[\d.,]*")


def parse_price(text: str | None, *, thousands: str = ".", decimal: str = ",") -> float | None:
    """
    Parse the first number in ``text`` using the given separators.

    Returns None when there is no number or the value is not positive
    ("U$S 0" is how some stores display "consultar precio").
    """
    if not text:
        return None
    match = _NUMBER.search(text.replace("\xa0", " "))
    if not match:
        return None
    raw = match.group(0).rstrip(thousands + decimal)
    if decimal in raw:
        integer, _, fraction = raw.rpartition(decimal)
    else:
        integer, fraction = raw, ""
    groups = integer.split(thousands)
    if len(groups) > 1 and not all(len(g) == 3 for g in groups[1:]):
        raise ValueError(f"Ambiguous price format: {text!r}")
    value = float("".join(groups) + ("." + fraction if fraction else ""))
    return value if value > 0 else None


def detect_currency(text: str | None, default: Currency = Currency.USD) -> Currency:
    """Map store currency markers to Currency. ``$`` alone means pesos uruguayos."""
    if not text:
        return default
    marker = text.upper().replace(" ", "")
    if any(tag in marker for tag in ("U$S", "US$", "USD", "U$D")):
        return Currency.USD
    if "UYU" in marker or "$U" in marker or marker.startswith("$"):
        return Currency.UYU
    return default
