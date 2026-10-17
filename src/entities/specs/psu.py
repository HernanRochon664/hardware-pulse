"""
Power supply title parser.

Family  = wattage + 80 Plus rating ("750 W 80 Plus Gold"); a title that does
          not state the rating (and whose series has no fixed one) goes to a
          family "certificación sin dato" of its own.
Product = brand + series (taxonomy ``psu_series``) + wattage + rating +
          version (PG5, V2...) + color ("Gigabyte UD GM 750W 80 Plus Gold PG5 V2
          White"). The rating is part of the identity: two listings of the same
          name with different ratings are not merged.

The wattage is the category marker: explicit ("750W") or read from the model
code of a known series (UD750GM, Atlas 600, TUF Gaming 1000G). Without a known
series the PSU is family_only: generic "Fuente 500W" titles are never merged.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, match_model
from src.entities.text import fold

_WATTS = re.compile(r"(?<![\d.])(\d{3,4}) ?w(?:atts?)?(?![a-z])")
_METALS = [
    ("titanium", re.compile(r"titanium|titanio")),
    ("platinum", re.compile(r"platinum|platium|planinum|platino")),
    ("gold", re.compile(r"\bgold\b|\boro\b")),
    ("silver", re.compile(r"\bsilver\b|\bplata\b")),
    ("bronze", re.compile(r"\bbronze\b|\bbronce\b")),
]
_PLUS = re.compile(r"\b80 ?\+|\b80 ?plus|\b80plus|\b80p\b|\bplus white\b")
_MARKER = re.compile(r"\bfuente|\bpsu\b|\bpower supply|\bmodular\b|\b80 ?\+|\b80 ?plus|\b80p\b")
# Cases sold with a PSU ("Gabinete ... + fuente 650W") are cases.
_OTHER = re.compile(r"\bgabinete|\bcase\b|\bchasis|\bchassis|\bnotebook|\blaptop|\bups\b")
_FULL = re.compile(r"\bfull[- ]?modular|\btotalmente modular|\bfully modular")
_SEMI = re.compile(r"\bsemi[- ]?modular")
_NON_MODULAR = re.compile(r"\bno modular|\bnon[- ]?modular")
_MODULAR = re.compile(r"\bmodular\b")
_WHITE = re.compile(r"(?<!plus )(?<!80 )(?<!\+ )\b(?:white|blanca|blanco|ice|wh)\b|\bpg5w\b")
_VARIANTS = [
    ("PG5", re.compile(r"\bpg5w?\b")),
    ("V2", re.compile(r"\bv2\b")),
    ("V3", re.compile(r"\bv3\b")),
    ("Shift", re.compile(r"\bshift\b")),
]
_ATX = re.compile(r"\batx ?(3[.,][01])\b")
_SFX = re.compile(r"\bsfx\b")

RATING_LABELS = {
    "white": "80 Plus",
    "bronze": "80 Plus Bronze",
    "silver": "80 Plus Silver",
    "gold": "80 Plus Gold",
    "platinum": "80 Plus Platinum",
    "titanium": "80 Plus Titanium",
}


def _rating(folded: str) -> tuple[str | None, list[str]]:
    metals = {name for name, pattern in _METALS if pattern.search(folded)}
    if len(metals) > 1:
        return None, ["ambiguous_rating"]
    notes = ["cybenetics"] if "cybenetics" in folded else []
    if metals:
        return metals.pop(), notes
    return ("white" if _PLUS.search(folded) else None), notes


def _modular(folded: str) -> str | None:
    if _FULL.search(folded):
        return "Full"
    if _SEMI.search(folded):
        return "Semi"
    if _NON_MODULAR.search(folded):
        return "No"
    return "Sí" if _MODULAR.search(folded) else None


def parse_psu(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    folded = fold(title)
    allowed = tax.brands_for(Category.PSU, tax.psu_series)
    brand = tax.detect_brand(title, allowed=allowed)
    if brand is None and store_brand in allowed:
        brand = store_brand
    series = match_model(tax.psu_series[brand], title) if brand in tax.psu_series else None
    if _OTHER.search(folded) or not (_MARKER.search(folded) or series):
        return ParseFailure("not_a_psu")

    watts = {int(m.group(1)) for m in _WATTS.finditer(folded) if 200 <= int(m.group(1)) <= 2500}
    if len(watts) > 1:
        return ParseFailure("ambiguous_wattage:" + "/".join(map(str, sorted(watts))))
    wattage = watts.pop() if watts else (series[2] if series else None)
    notes: list[str] = []
    if series and series[2] and wattage and series[2] != wattage:
        return ParseFailure("wattage_mismatch")
    if wattage is None:
        return ParseFailure("no_wattage")
    if not 200 <= wattage <= 2500:
        return ParseFailure("bad_wattage")

    rating, rating_notes = _rating(folded)
    notes += rating_notes
    attrs = series[1] if series else {}
    if rating is None and "ambiguous_rating" not in notes and attrs.get("rating"):
        rating = str(attrs["rating"])
        notes.append("rating_from_series")
    modular = _modular(folded)
    if modular is None and attrs.get("modular"):
        modular = {"full": "Full", "semi": "Semi"}.get(str(attrs["modular"]))
    white = bool(_WHITE.search(folded))
    variants = [name for name, pattern in _VARIANTS if pattern.search(folded)]
    atx = _ATX.search(folded)

    rating_label = RATING_LABELS.get(rating or "", "certificación sin dato")
    family_name = f"{wattage} W {rating_label}"
    family_id = f"psu:{wattage}w-{rating or 'na'}"
    name = join_name(
        brand,
        series[0] if series else None,
        f"{wattage}W",
        RATING_LABELS.get(rating or ""),
        *variants,
        "White" if white else None,
    )

    fingerprint = None
    if brand and series:
        fingerprint = "|".join(
            [
                "psu",
                brand.lower(),
                series[0].lower(),
                str(wattage),
                rating or "",
                "+".join(v.lower() for v in variants),
                "white" if white else "",
            ]
        )
    else:
        notes.append("unknown_brand" if brand is None else "unknown_series")

    family_specs = {"wattage_w": wattage, "rating": RATING_LABELS.get(rating or "")}
    return Parsed(
        category=Category.PSU,
        family_id=family_id,
        family_name=family_name,
        name=name,
        brand=brand,
        line=series[0] if series else None,
        fingerprint=fingerprint,
        specs={
            **family_specs,
            "modular": modular,
            "atx_version": atx.group(1).replace(",", ".") if atx else None,
            "form": "SFX" if _SFX.search(folded) else None,
            "white": white,
            "variants": variants,
        },
        family_specs=family_specs,
        notes=notes,
    )
