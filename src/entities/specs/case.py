"""
PC case title parser ("light" category, see docs/decisions.md ADR-09).

Family  = brand + model ("Cougar FV270"): cases are not interchangeable across
          brands, so the family groups the colors and editions of one model.
Product = family + color + RGB + bundled PSU ("Cougar FV270 RGB White").

The model comes from the taxonomy (``case_models``) or, for brands without a
listed model, from a model code in the title (TE-1330G). Black is the default
color and is ignored, as for GPUs. Supported form factors are a spec only when
the title states them.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, match_model
from src.entities.text import fold, slug

_MARKER = re.compile(r"\bgabinete|\bcase\b|\bchasis|\bchassis|\btorre\b|\btower\b")
_COLORS = [
    ("White", re.compile(r"\b(?:white|blanco|blanca|wh|ice|snow)\b|-white\b")),
    ("Pink", re.compile(r"\b(?:pink|rosa|rosado)\b")),
    ("Sand", re.compile(r"\bsand\b|\barena\b")),
    ("Gray", re.compile(r"\b(?:gray|grey|gris)\b")),
]
_RGB = re.compile(r"\ba?rgb\b")
_PSU = re.compile(r"(?:\+|\bcon|\bc/|\by)? ?fuente(?: de(?: poder)?)?(?: deepcool)? ?(\d{3,4}) ?w")
_FORMS = [
    ("Full tower", re.compile(r"\bfull[- ]?tower\b")),
    ("Mid tower", re.compile(r"\bmid[- ]?tower\b|\bmidtower\b")),
    ("E-ATX", re.compile(r"\be-?atx\b")),
    ("Micro ATX", re.compile(r"\bmicro[- ]?atx\b|\bm-?atx\b")),
    ("Mini ITX", re.compile(r"\bmini[- ]?itx\b|\bitx\b")),
    ("ATX", re.compile(r"\batx\b")),
]
_SPEC_TOKEN = re.compile(r"^\d+(?:w|mm|cm|gb|f)$|^(?:atx|matx)")
# Edition words that follow a model code and change the product (HAF700 vs HAF700 EVO).
_EDITIONS = {"evo", "pro", "plus", "mini", "max", "lite", "elite", "ultra", "ii", "iii", "v2", "v3"}


def _model_code(folded: str, brand: str) -> str | None:
    """First model-code-looking token (TE-1330G), without brand or color words in it."""
    brand_tokens = set(fold(brand).replace("-", " ").split())
    colors = {"white", "black", "blanco", "negro", "blanca", "negra", "wh", "bk"}
    tokens = re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)*", folded)
    for i, token in enumerate(tokens):
        kept = [p for p in token.split("-") if p not in brand_tokens and p not in colors]
        for part in ["-".join(kept), *kept]:
            packed = part.replace("-", "")
            digits = sum(c.isdigit() for c in packed)
            if (
                len(packed) >= 4
                and digits >= 2
                and digits < len(packed)
                and packed not in brand_tokens
                and not _SPEC_TOKEN.search(packed)
            ):
                edition = tokens[i + 1] if i + 1 < len(tokens) else ""
                return f"{part} {edition}".upper() if edition in _EDITIONS else part.upper()
    return None


def parse_case(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    folded = fold(title)
    allowed = tax.brands_for(Category.CASE, tax.case_models)
    brand = tax.detect_brand(title, allowed=allowed)
    if brand is None and store_brand in allowed:
        brand = store_brand
    model = match_model(tax.case_models[brand], title) if brand in tax.case_models else None
    if not (model or _MARKER.search(folded)):
        return ParseFailure("not_a_case")
    if brand is None:
        return ParseFailure("unknown_brand")
    notes: list[str] = []
    name_model = model[0] if model else _model_code(folded, brand)
    if name_model is None:
        return ParseFailure("unknown_model")
    if model is None:
        notes.append("model_from_code")

    color = next((label for label, pattern in _COLORS if pattern.search(folded)), None)
    rgb = bool(_RGB.search(folded))
    psu = _PSU.search(folded)
    psu_w = int(psu.group(1)) if psu else None
    form = next((label for label, pattern in _FORMS if pattern.search(folded)), None)

    family_name = f"{brand} {name_model}"
    name = join_name(
        family_name,
        "RGB" if rgb and "RGB" not in name_model.upper() else None,
        color,
        f"+ fuente {psu_w}W" if psu_w else None,
    )
    fingerprint = "|".join(
        [
            "case",
            brand.lower(),
            name_model.lower(),
            (color or "").lower(),
            "rgb" if rgb else "",
            str(psu_w or ""),
        ]
    )
    return Parsed(
        category=Category.CASE,
        family_id=f"case:{slug(family_name)}",
        family_name=family_name,
        name=name,
        brand=brand,
        line=name_model,
        fingerprint=fingerprint,
        specs={"form": form, "color": color, "rgb": rgb, "psu_w": psu_w},
        family_specs={"model": family_name},
        notes=notes,
    )
