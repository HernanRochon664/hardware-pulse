"""
SSD title parser.

Family  = interface class + capacity ("SSD NVMe Gen4 1TB").
Product = brand + model + capacity + interface + variant (heatsink, 2230...).

Capacities are exact (960GB != 1TB, 480GB != 500GB) except binary/decimal
spellings of the same drive (1024GB == 1TB). Hard drives, enclosures and USB
sticks that share the store category are rejected before parsing.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, fold_tokens, match_alias
from src.entities.text import fold, slug, spaced

_CAPACITY = re.compile(r"(\d+(?:[.,]\d{1,2})?)\s?(gb|tb|g|t)\b(?!\s?/\s?s|ps)")
_SSD_MARKERS = re.compile(r"\b(ssd|solido|nvme|m ?\.? ?2|pcie|sata)\b")
_EXTERNAL = re.compile(r"\b(externo|externa|portable|portatil|usb)\b")

IFACE_LABEL = {
    "nvme5": "NVMe Gen5",
    "nvme4": "NVMe Gen4",
    "nvme3": "NVMe Gen3",
    "nvme": "NVMe",
    "sata": "SATA",
    "external": "externo",
}

_SSD_BRANDS = {
    "Kingston",
    "ADATA",
    "Corsair",
    "Patriot",
    "Netac",
    "Hiksemi",
    "KingDian",
    "Lexar",
    "MSI",
    "Western Digital",
    "TeamGroup",
    "Verbatim",
    "ValueTech",
    "Samsung",
    "Crucial",
    "Biostar",
    "SK Hynix",
    "Ediloca",
    "Raver",
    "HPE",
    "Gigabyte",
}


def _capacity_gb(folded: str) -> int | None:
    for m in _CAPACITY.finditer(folded):
        value = float(m.group(1).replace(",", "."))
        gb = value * 1000 if m.group(2).startswith("t") else value
        if gb < 64:
            continue  # "6Gb/s", cache sizes...
        for binary, decimal in ((1024, 1000), (2048, 2000), (4096, 4000), (8192, 8000)):
            if round(gb) == binary:
                gb = decimal
        return round(gb)
    return None


def capacity_label(gb: int) -> str:
    if gb >= 1000:
        tb = gb / 1000
        return f"{tb:g}TB"
    return f"{gb}GB"


def _iface_from_title(text: str, folded: str) -> str | None:
    if _EXTERNAL.search(folded):
        return "external"
    for gen in ("5", "4", "3"):
        if re.search(rf"\b(pcie? ?{gen}( 0)?|gen ?{gen}( 0)?|{gen} x 4)\b", text):
            return f"nvme{gen}"
    if re.search(r"\bsata\b", text) or re.search(r"\b2 5\b", text):
        return "sata"
    if "nvme" in text or re.search(r"\bpcie?\b", text):
        return "nvme"
    return None


def parse_ssd(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    text = spaced(title)
    folded = fold(title)
    brand = tax.detect_brand(title, allowed=set(tax.ssd_models) | _SSD_BRANDS)
    model = match_alias(tax.ssd_models[brand], title) if brand in tax.ssd_models else None
    if not _SSD_MARKERS.search(text) and model is None:
        return ParseFailure("not_an_ssd")
    capacity = _capacity_gb(folded)
    if capacity is None:
        return ParseFailure("no_capacity")

    iface = _iface_from_title(text, folded)
    default = tax.ssd_ifaces.get((brand or "", model or ""))
    if default and (iface is None or (iface == "nvme" and default.startswith("nvme"))):
        iface = default
    if iface is None:
        return ParseFailure("unknown_interface")

    toks = set(fold_tokens(title)) | set(text.split())
    variants: list[str] = []
    if (
        re.search(r"(con disipador|disipado|heatsink|c/disip)", folded)
        and "sin disipador" not in folded
    ):
        variants.append("Disipador")
    if "hydro" in toks:
        variants.append("Hydro X")
    for form in ("2230", "2242"):
        if form in toks and "Mini" not in (model or ""):
            variants.append(form)
    if model and model.startswith("XS") and "red" in toks:
        variants.append("Red")

    cap = capacity_label(capacity)
    family_name = f"SSD {IFACE_LABEL[iface]} {cap}"
    family_id = f"ssd:{iface}-{slug(cap)}"
    name = join_name(brand, model, cap, f"({IFACE_LABEL[iface]})", *variants)
    notes: list[str] = []
    fingerprint = None
    if brand and model:
        fingerprint = "|".join(
            [
                "ssd",
                brand.lower(),
                model.lower(),
                str(capacity),
                iface,
                "+".join(v.lower() for v in variants),
            ]
        )
    else:
        notes.append("unknown_brand" if brand is None else "unknown_model")

    family_specs = {"interface": iface, "capacity_gb": capacity}
    return Parsed(
        category=Category.SSD,
        family_id=family_id,
        family_name=family_name,
        name=name,
        brand=brand,
        line=model,
        fingerprint=fingerprint,
        specs={**family_specs, "variants": variants},
        family_specs=family_specs,
        notes=notes,
    )
