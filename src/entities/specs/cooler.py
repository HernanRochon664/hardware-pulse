"""
CPU cooler title parser.

Family  = liquid (AIO) by radiator size ("Refrigeración líquida 360 mm"), or
          air by class: one tower, two towers, top-flow / low profile.
Product = brand + model (taxonomy ``cooler_models``) + radiator + color + RGB
          + LCD ("DeepCool LE V2 360mm ARGB White").

Case fans, fan hubs, controllers and thermal paste share the store category:
without a known cooler model, a title that talks about fans is not a CPU
cooler. An AIO of an unknown model still has a family (its radiator); an air
cooler of an unknown model cannot be classified and stays unparsed.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, match_model
from src.entities.text import fold, spaced

_AIO = re.compile(
    r"water ?cool|liquid|liquida|liquido|\baio\b|refrigeracion liquida|enfriamiento liquido"
)
_AIR = re.compile(r"\bdisipador|\bcooler\b|\bheatsink|\bair cooler")
_FAN = re.compile(r"\bfan(?:s|es)?\b|\bventilador(?:es)?\b|\bhub\b")
_RADIATORS = (120, 240, 280, 360, 420)
_WHITE = re.compile(r"\b(?:white|blanco|blanca|wh|wt|ice)\b")
_RGB = re.compile(r"\ba?rgb\b")
_LCD = re.compile(r"\blcd\b|\bpantalla\b|\bdisplay\b")

KIND_FAMILIES = {
    "air_low": ("cooler:air-low", "Cooler de aire top-flow / bajo perfil"),
    "air_tower": ("cooler:air-tower", "Cooler de aire de una torre"),
    "air_dual": ("cooler:air-dual", "Cooler de aire de doble torre"),
}
KIND_LABELS = {
    "aio": "Refrigeración líquida",
    "air_low": "Aire top-flow / bajo perfil",
    "air_tower": "Aire, una torre",
    "air_dual": "Aire, doble torre",
}


def _radiator(text: str, captured: int | None, attrs: dict) -> int | None:
    if attrs.get("radiator"):
        return int(attrs["radiator"])
    if captured in _RADIATORS:
        return captured
    sizes = [int(t) for t in text.split() if t.isdigit() and int(t) in _RADIATORS]
    return max(sizes) if sizes else None


def parse_cooler(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    folded = fold(title)
    text = spaced(title)
    allowed = tax.brands_for(Category.COOLER, tax.cooler_models)
    brand = tax.detect_brand(title, allowed=allowed)
    if brand is None and store_brand in allowed:
        brand = store_brand
    model = match_model(tax.cooler_models[brand], title) if brand in tax.cooler_models else None

    if model:
        kind = str(model[1]["kind"])
    elif _AIO.search(folded):
        kind = "aio"
    elif _FAN.search(folded) or not _AIR.search(folded):
        return ParseFailure("not_a_cpu_cooler")
    else:
        return ParseFailure("unknown_air_model")

    notes: list[str] = []
    radiator = None
    if kind == "aio":
        radiator = _radiator(text, model[2] if model else None, model[1] if model else {})
        if radiator is None:
            return ParseFailure("no_radiator")
        family_id, family_name = f"cooler:aio-{radiator}", f"Refrigeración líquida {radiator} mm"
    else:
        family_id, family_name = KIND_FAMILIES[kind]

    white = bool(_WHITE.search(folded)) or bool(model and model[1].get("white"))
    rgb = bool(_RGB.search(folded))
    lcd = bool(_LCD.search(folded))
    name = join_name(
        brand,
        model[0] if model else None,
        f"{radiator}mm" if radiator else None,
        "LCD" if lcd else None,
        "ARGB" if rgb else None,
        "White" if white else None,
    )
    fingerprint = None
    if brand and model:
        fingerprint = "|".join(
            [
                "cooler",
                brand.lower(),
                model[0].lower(),
                str(radiator or ""),
                "white" if white else "",
                "rgb" if rgb else "",
                "lcd" if lcd else "",
            ]
        )
    else:
        notes.append("unknown_brand" if brand is None else "unknown_model")

    family_specs = {"kind": KIND_LABELS[kind], "radiator_mm": radiator}
    return Parsed(
        category=Category.COOLER,
        family_id=family_id,
        family_name=family_name,
        name=name,
        brand=brand,
        line=model[0] if model else None,
        fingerprint=fingerprint,
        specs={**family_specs, "white": white, "rgb": rgb, "lcd": lcd},
        family_specs=family_specs,
        notes=notes,
    )
