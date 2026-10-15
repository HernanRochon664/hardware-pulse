"""
RAM title parser.

Family  = DDR generation + total capacity + speed + form factor
          ("DDR5 32GB 6000 MT/s DIMM").
Product = brand + line + family specs + kit layout + RGB/white.

A 2x8GB kit is a 16GB product (v1 read it as an 8GB module), and SODIMM
(notebook) memory never shares a family with desktop DIMMs.
"""

from __future__ import annotations

import re

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, fold_tokens, match_alias
from src.entities.text import fold, slug, spaced

_GEN = re.compile(r"\bddr ([345])\b")
_KIT = [
    re.compile(r"\b([248]) x (\d{1,3})(?: gb)?\b"),  # 2x16GB, (2X8)
    re.compile(r"\b(\d{1,3}) gb ?x ([248])\b"),  # 16GBx2
    re.compile(r"\b(\d{1,3}) gbx ([248])\b"),
]
_CAPACITY = re.compile(r"\b(\d{1,3}) gb\b")
_PC_RATE = re.compile(r"\bpc ?[345]? (\d{5})\b")  # PC4-25600 -> 3200
_PC_SPEED = re.compile(r"\bpc ?(\d{4})\b")  # PC3200 -> 3200
_NUMBER = re.compile(r"\b(\d{4})\b")
_VALID_CAPACITY = {1, 2, 4, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256}
# Speeds encoded in manufacturer part numbers (spaced text):
#   Kingston KF556C40 -> "kf 556 c 40" -> 5600;  KVR56U46 -> "kvr 56 u 46" -> 5600
#   Netac NTSWD5P60DP -> "ntswd 5 p 60 dp" -> 6000
#   Patriot PSD516G560081 -> "psd 516 g 560081" -> 5600
_MPN_SPEEDS = [
    re.compile(r"\bkf ([45])(\d{2}) [cr] \d{2}\b"),
    re.compile(r"\b(?:kvr|kcp) (\d{2}) [usn] \d{2}\b"),
    re.compile(r"\b[a-z]*d ([45]) p (\d{2}) [a-z]{2}\b"),
    re.compile(r"\bp[a-z]{1,3} \d{3,4} g (\d{3})\d{2,3}\b"),
]


def _speed(text: str, valid: set[int]) -> int | None:
    for m in _PC_RATE.finditer(text):
        rate = int(m.group(1)) / 8
        nearest = min(valid, key=lambda s: abs(s - rate))
        if abs(nearest - rate) <= 15:
            return nearest
    for m in list(_PC_SPEED.finditer(text)) + list(_NUMBER.finditer(text)):
        value = int(m.group(1))
        if value in valid:
            return value
    for pattern in _MPN_SPEEDS:
        m = pattern.search(text)
        if m:
            digits = m.group(m.lastindex or 1)
            value = int(digits) * (100 if len(digits) == 2 else 10)
            if value in valid:
                return value
    return None


def _kit(text: str) -> tuple[int, int] | None:
    for i, pattern in enumerate(_KIT):
        for m in pattern.finditer(text):
            a, b = int(m.group(1)), int(m.group(2))
            modules, size = (a, b) if i == 0 else (b, a)
            if size in _VALID_CAPACITY and modules in (2, 4, 8):
                return modules, size
    return None


def _form_factor(title: str, text: str) -> str:
    folded = fold(title)
    if re.search(r"\b(so ?dimm|soimmm|so-dimm|notebook|laptop|portatil)\b", folded) or (
        "sodimm" in text.replace(" ", "")
    ):
        return "SODIMM"
    if "cudimm" in text:
        return "CUDIMM"
    if re.search(r"\b(r ?dimm|r-dimm|ecc|registered)\b", folded) or re.search(
        r"\bkf5\d{2}r\d{2}", folded
    ):
        return "RDIMM"
    return "DIMM"


def _gen_from_speed(speed: int | None) -> str | None:
    if speed is None:
        return None
    if speed >= 4800:
        return "DDR5"
    if speed >= 2133:
        return "DDR4"
    return "DDR3"


def parse_ram(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    text = spaced(title)
    notes: list[str] = []
    speed = _speed(text, tax.ram_speeds)
    gen_match = _GEN.search(text)
    gen = f"DDR{gen_match.group(1)}" if gen_match else _gen_from_speed(speed)
    if gen is None:
        return ParseFailure("no_ddr_generation")
    if not gen_match:
        notes.append("generation_inferred")

    kit = _kit(text)
    capacities = [int(m.group(1)) for m in _CAPACITY.finditer(text)]
    capacities = [c for c in capacities if c in _VALID_CAPACITY]
    if kit:
        total = kit[0] * kit[1]
    elif capacities:
        total = capacities[0]
    else:
        return ParseFailure("no_capacity")
    if speed is None:
        return ParseFailure("no_speed")

    form = _form_factor(title, text)
    family_name = f"{gen} {total}GB {speed} MT/s {form}"
    family_id = f"ram:{slug(f'{gen}-{total}gb-{speed}-{form}')}"

    brand = tax.detect_brand(title, allowed=set(tax.ram_lines) | _RAM_BRANDS)
    line = match_alias(tax.ram_lines[brand], title) if brand in tax.ram_lines else None
    if brand == "Kingston" and line == "Fury Beast" and form == "SODIMM":
        line = "Fury Impact"
    toks = set(fold_tokens(title)) | set(text.split())
    rgb = "rgb" in toks or (line or "").endswith("RGB") or line == "Spectrix D35G"
    white = bool(toks & {"white", "blanca", "blanco", "wh", "wt"})
    kit_label = f"{kit[0]}x{kit[1]}GB" if kit else None

    name = join_name(
        brand,
        line,
        gen,
        f"{total}GB",
        f"({kit_label})" if kit_label else None,
        f"{speed} MT/s",
        None if form == "DIMM" else form,
        "RGB" if rgb and not (line or "").endswith("RGB") else None,
        "White" if white else None,
    )
    fingerprint = None
    if brand and line:
        fingerprint = "|".join(
            [
                "ram",
                brand.lower(),
                line.lower(),
                gen.lower(),
                str(total),
                kit_label or "1x",
                str(speed),
                form.lower(),
                "rgb" if rgb else "",
                "white" if white else "",
            ]
        )
    else:
        notes.append("unknown_brand" if brand is None else "unknown_line")

    family_specs = {"generation": gen, "capacity_gb": total, "speed_mts": speed, "form": form}
    return Parsed(
        category=Category.RAM,
        family_id=family_id,
        family_name=family_name,
        name=name,
        brand=brand,
        line=line,
        fingerprint=fingerprint,
        specs={**family_specs, "kit": kit_label, "rgb": rgb, "white": white},
        family_specs=family_specs,
        notes=notes,
    )


_RAM_BRANDS = {
    "Kingston",
    "Corsair",
    "ADATA",
    "Patriot",
    "Netac",
    "Hiksemi",
    "Hikvision",
    "TeamGroup",
    "G.Skill",
    "Crucial",
    "Lexar",
    "Samsung",
    "ValueTech",
    "Oditox",
    "Aimerican Telekinesis",
}
