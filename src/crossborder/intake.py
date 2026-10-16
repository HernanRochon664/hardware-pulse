"""
What the bookmarklet sends from a US store page, parsed into numbers, and the
match of that product with the catalog.

Matching never guesses (same rule as entity resolution):
1. the same manufacturer part number (MPN);
2. the same exact product according to the strict title parsers (one category
   only, one product with that fingerprint);
3. otherwise only the same family, shown as "equivalente".
"""

from __future__ import annotations

import re
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from src.crossborder.config import CrossBorderConfig
from src.domain.models import Category, Condition
from src.entities.mpn import extract_mpn, normalize_mpn
from src.entities.resolver import resolve_title
from src.entities.taxonomy import Taxonomy
from src.scrapers.prices import parse_price

_NUMBER = r"\d[\d.,]*"
_WEIGHT_UNITS = {
    "kg": 1.0,
    "kilogram": 1.0,
    "kilogramo": 1.0,
    "g": 0.001,
    "gram": 0.001,
    "gramo": 0.001,
    "lb": 0.45359237,
    "lbs": 0.45359237,
    "pound": 0.45359237,
    "libra": 0.45359237,
    "oz": 0.028349523125,
    "ounce": 0.028349523125,
    "onza": 0.028349523125,
}
_LENGTH_UNITS = {
    "cm": 1.0,
    "centimeter": 1.0,
    "centimetro": 1.0,
    "centímetro": 1.0,
    "mm": 0.1,
    "millimeter": 0.1,
    "milimetro": 0.1,
    "milímetro": 0.1,
    "in": 2.54,
    "inch": 2.54,
    "inche": 2.54,
    "pulgada": 2.54,
    '"': 2.54,
}
_WEIGHT_RE = re.compile(
    rf"({_NUMBER})\s*(kilogramos?|kilograms?|kg|gramos?|grams?|g|pounds?|libras?|lbs?|"
    r"ounces?|onzas?|oz)\b",
    re.IGNORECASE,
)
_DIMS_RE = re.compile(
    rf"({_NUMBER})\s*[x×]\s*({_NUMBER})\s*[x×]\s*({_NUMBER})\s*"
    r"(centímetros|centimetros|centimeters|cm|milímetros|milimetros|millimeters|mm|"
    r'inches|inch|in|pulgadas|")?',
    re.IGNORECASE,
)


_US_FORMAT = (",", ".")
_LATIN_FORMAT = (".", ",")


def parse_number(text: str, *, latin_first: bool = False) -> float | None:
    """
    First number of a page text: '1,099.99' (US format) or '1.099,99' (pages
    shown in pesos or euros, tried first with ``latin_first``). None if
    missing, zero or ambiguous in both formats.
    """
    formats = (_LATIN_FORMAT, _US_FORMAT) if latin_first else (_US_FORMAT, _LATIN_FORMAT)
    for thousands, decimal in formats:
        try:
            return parse_price(text, thousands=thousands, decimal=decimal)
        except ValueError:
            continue
    return None


def parse_money(text: str | None) -> tuple[float | None, str | None]:
    """
    Price and currency ('USD', 'UYU', 'EUR' or None) of a price on a US page,
    where a bare '$' means dollars (unlike Uruguayan stores, see detect_currency).
    """
    if not text:
        return None, None
    upper = text.upper()
    if "UYU" in upper or "$U" in upper or "UY$" in upper:
        currency = "UYU"
    elif "US$" in upper or "USD" in upper or "U$S" in upper or "$" in upper:
        currency = "USD"
    elif "€" in upper or "EUR" in upper:
        currency = "EUR"
    else:
        currency = None
    return parse_number(text, latin_first=currency in ("UYU", "EUR")), currency


def _unit(table: Mapping[str, float], word: str) -> float | None:
    key = word.lower().rstrip("s") if len(word) > 2 else word.lower()
    return table.get(key, table.get(word.lower()))


def parse_weight_kg(text: str | None) -> float | None:
    """'2.43 pounds' / '1,1 kg' / '39 ounces' -> kg."""
    if not text:
        return None
    match = _WEIGHT_RE.search(text)
    if not match:
        return None
    value = parse_number(match.group(1))
    factor = _unit(_WEIGHT_UNITS, match.group(2))
    if value is None or factor is None or value <= 0:
        return None
    return round(value * factor, 3)


def parse_dims_cm(text: str | None) -> tuple[float, float, float] | None:
    """'13.5 x 9.84 x 3.66 inches' / '34 x 25 x 9 cm' -> cm (inches when no unit is given)."""
    if not text:
        return None
    match = _DIMS_RE.search(text)
    if not match:
        return None
    values = [parse_number(match.group(i)) for i in (1, 2, 3)]
    factor = _unit(_LENGTH_UNITS, match.group(4)) if match.group(4) else 2.54
    if factor is None or any(v is None or v <= 0 for v in values):
        return None
    length, width, height = (round(float(v or 0) * factor, 1) for v in values)
    return length, width, height


_AMAZON_SELLER = re.compile(
    r"amazon(\.com)?( export sales)?(,? (services|inc)\.?)?( llc)?\.?", re.IGNORECASE
)


def sold_by_amazon(text: str | None) -> bool | None:
    """True for 'Amazon.com' / 'Amazon.com Services LLC'; None when unknown."""
    if not text or not text.strip():
        return None
    cleaned = re.sub(r"^(sold by|vendido por)\s*:?\s*", "", text.strip(), flags=re.IGNORECASE)
    return bool(_AMAZON_SELLER.fullmatch(cleaned.strip()))


@dataclass
class Intake:
    """A US offer captured from a store page."""

    title: str
    url: str | None
    store_id: str
    price_usd: float | None
    mpn: str | None
    weight_kg: float | None
    dims_cm: tuple[float, float, float] | None
    sold_by: str | None
    warnings: list[str] = field(default_factory=list)


def parse_intake(params: Mapping[str, str], cfg: CrossBorderConfig) -> Intake | None:
    """Query params of the bookmarklet (t, p, u, sold, w, d, mpn) -> Intake, or None."""
    title = (params.get("t") or "").strip()
    price_text = params.get("p")
    if not title and not price_text:
        return None
    warnings: list[str] = []
    url = params.get("u") or None
    host = urllib.parse.urlsplit(url).hostname if url else None
    store = cfg.stores.store_for_host(host) if host else None
    store_id = store.id if store else "other"
    sold_by = (params.get("sold") or "").strip() or None
    if store_id == "amazon":
        amazon_sells = sold_by_amazon(sold_by)
        if amazon_sells is False:
            store_id = "amazon_marketplace"
        elif amazon_sells is None:
            warnings.append(
                "No se pudo leer quién vende el producto en Amazon: se asume «Vendido por "
                "Amazon.com». Si es un vendedor externo, cambialo."
            )

    price, currency = parse_money(price_text)
    if price_text and price is None:
        warnings.append("No se pudo leer el precio: ingresalo a mano.")
    if currency and currency != "USD":
        warnings.append(
            f"El precio está en {currency}: cambiá la moneda de la tienda a dólares (USD) y "
            "volvé a enviarlo."
        )
        price = None

    weight = parse_weight_kg(params.get("w")) or parse_weight_kg(params.get("d"))
    dims = parse_dims_cm(params.get("d"))
    mpn = (params.get("mpn") or "").strip() or None
    return Intake(title, url, store_id, price, mpn, weight, dims, sold_by, warnings)


# ---------------------------------------------------------------------------
# Matching with the catalog
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CatalogProduct:
    id: int
    name: str
    category: str
    family_id: str | None
    fingerprint: str | None
    mpn: str | None


@dataclass(frozen=True)
class CatalogMatch:
    kind: Literal["mpn", "exact", "family"]
    product_ids: tuple[int, ...]
    family_id: str | None
    category: str
    used: bool = False

    @property
    def is_exact(self) -> bool:
        return self.kind in ("mpn", "exact")


def match_catalog(
    title: str,
    mpn: str | None,
    products: Sequence[CatalogProduct],
    taxonomy: Taxonomy | None = None,
) -> CatalogMatch | None:
    """The catalog product(s) that a US title describes, or None. Never guesses."""
    codes = {normalize_mpn(code) for code in (mpn, extract_mpn(title)) if code}
    codes = {code for code in codes if len(code) >= 6}
    if codes:
        hits = [p for p in products if p.mpn and normalize_mpn(p.mpn) in codes]
        if len({p.id for p in hits}) == 1:
            return CatalogMatch("mpn", (hits[0].id,), hits[0].family_id, hits[0].category)

    parsed = []
    for category in Category:
        resolution = resolve_title(category, title, taxonomy=taxonomy)
        if resolution.parsed is not None:
            parsed.append((category, resolution))
    if len(parsed) != 1:
        return None
    category, resolution = parsed[0]
    info = resolution.parsed
    assert info is not None
    used = resolution.condition is not Condition.NEW
    if info.fingerprint:
        same = tuple(p.id for p in products if p.fingerprint == info.fingerprint)
        if same:
            return CatalogMatch("exact", same, info.family_id, category.value, used)
    family = tuple(p.id for p in products if p.family_id == info.family_id)
    if family:
        return CatalogMatch("family", family, info.family_id, category.value, used)
    return None
