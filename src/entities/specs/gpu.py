"""
GPU title parser.

Family  = chip + VRAM ("RTX 5060 Ti 16GB"): the market-level unit.
Product = brand + chip + VRAM + product line + modifiers (OC, White, LP, SFF,
          V2, Plus, EVO, ITX, memory type for low-end chips).

The chip suffix is exact: "RTX 5070 Ti" is never read as "RTX 5070", which
was the main cause of mixed prices in v1.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.domain.models import Category
from src.entities.specs import Parsed, ParseFailure, join_name
from src.entities.taxonomy import Taxonomy, fold_tokens, match_alias
from src.entities.text import compact, slug, spaced

_NVIDIA = re.compile(r"\b(rtx|gtx|gt) (\d{3,4})( ti)?( super)?\b")
_NV_210 = re.compile(r"\b(?:g|n) (210)\b")
_GEFORCE_BARE = re.compile(r"\bgeforce (\d{4})( ti)?( super)?\b")
_AMD = re.compile(r"\b(?:rx|r) (\d{3,4})( xtx| xt| gre)?\b")
_RADEON_BARE = re.compile(r"\bradeon (\d{4})( xtx| xt| gre)?\b")
_ARC = re.compile(r"\b([ab]) (\d{3})\b")
# Gigabyte part numbers: GV-N506T... = RTX 5060 Ti, GV-R906X... = RX 9060 XT, GV-R76 = RX 7600.
_GV_NVIDIA = re.compile(r"\bgv (?:n )?(\d{2,4})(?: ([a-z]+))?")
_GV_AMD = re.compile(r"\bgv r (\d{2,4})(?: ([a-z]+))?")
_VRAM = re.compile(r"\b(\d{1,2}) (?:gb|g|gd|bb)\b")
_VALID_VRAM = {1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 32}
_MEMTYPE = re.compile(r"\b(s?ddr|gddr|d) ?([3-7])\b")

GPU_BRANDS_EXTRA = {"Gainward", "EVGA", "Galax", "Zotac", "PNY"}

_MOD_ORDER = ["OC", "White", "LP", "SFF", "V2", "Plus", "EVO", "ITX"]


@dataclass(frozen=True)
class _Chip:
    base: str  # "RTX 5060"
    suffix: str  # "Ti", "XT", "" ...

    @property
    def name(self) -> str:
        return f"{self.base} {self.suffix}".strip()


def _nvidia_chip(prefix: str, number: str, ti: str | None, super_: str | None) -> _Chip:
    suffix = " ".join(s for s in ("Ti" if ti else "", "Super" if super_ else "") if s)
    return _Chip(f"{prefix.upper()} {number}", suffix)


def _geforce_prefix(number: int) -> str:
    return "RTX" if number >= 2000 else ("GTX" if number >= 1050 else "GT")


def _find_chips(text: str) -> list[_Chip]:
    chips: list[_Chip] = []
    for m in _NVIDIA.finditer(text):
        chips.append(_nvidia_chip(m.group(1), m.group(2), m.group(3), m.group(4)))
    if _NV_210.search(text):
        chips.append(_Chip("GT 210", ""))
    for m in _GEFORCE_BARE.finditer(text):
        n = int(m.group(1))
        chips.append(_nvidia_chip(_geforce_prefix(n), m.group(1), m.group(2), m.group(3)))
    for m in list(_AMD.finditer(text)) + list(_RADEON_BARE.finditer(text)):
        if text[: m.start()].endswith("gv "):
            continue  # Gigabyte part number, decoded by _GV_AMD below
        suffix = (m.group(2) or "").strip().upper()
        chips.append(_Chip(f"RX {m.group(1)}", suffix))
    if " arc" in f" {text}" or "intel" in text:
        for m in _ARC.finditer(text):
            chips.append(_Chip(f"Arc {m.group(1).upper()}{m.group(2)}", ""))
    for m in _GV_AMD.finditer(text):
        digits, rest = m.group(1), m.group(2) or ""
        number = digits.ljust(4, "0") if len(digits) < 4 else digits
        chips.append(_Chip(f"RX {number}", "XT" if rest.startswith("x") else ""))
    for m in _GV_NVIDIA.finditer(text):
        digits, rest = m.group(1), m.group(2) or ""
        if text[m.start() :].startswith("gv r "):
            continue
        if len(digits) == 3:  # N506T -> 5060 Ti, N507T -> 5070 Ti
            chips.append(_Chip(f"RTX {digits}0", "Ti" if rest.startswith("t") else ""))
        elif len(digits) == 4:
            chips.append(_Chip(f"RTX {digits}", ""))
    return chips


def _pick_chip(chips: list[_Chip]) -> _Chip | ParseFailure:
    if not chips:
        return ParseFailure("no_chip")
    bases = {c.base for c in chips}
    if len(bases) > 1:
        return ParseFailure(f"ambiguous_chip:{'/'.join(sorted(bases))}")
    # Same base (e.g. "RX 9070" and "RX 9070 XT"): the suffixed reading wins,
    # glued part numbers ("R9070XTAORUS") often hide the suffix from the regex.
    suffixes = {c.suffix for c in chips if c.suffix}
    if len(suffixes) > 1:
        return ParseFailure(f"ambiguous_suffix:{'/'.join(sorted(suffixes))}")
    return _Chip(chips[0].base, next(iter(suffixes), ""))


def _vram(text: str, allowed: list[int] | None) -> int | None:
    found = [int(m.group(1)) for m in _VRAM.finditer(text) if int(m.group(1)) in _VALID_VRAM]
    if allowed:
        found = [v for v in found if v in allowed]
    return found[0] if found else None


def _modifiers(title: str, text: str) -> set[str]:
    toks = set(fold_tokens(title)) | set(text.split())
    packed = compact(title)
    mods: set[str] = set()
    if (
        "oc" in toks
        or re.search(r"(eagle|gaming|aero|windforce|aorus|wf\d|max|twin|x2)oc", packed)
        or re.search(r"\bo (?:\d{1,2}) g\b", text)  # ASUS part numbers: O8G, O16G
    ):
        mods.add("OC")
    if toks & {"white", "blanca", "blanco", "wh", "wt", "w", "ice"} or "ocice" in packed:
        mods.add("White")
    if "lp" in toks or "low profile" in text or "bajo perfil" in text:
        mods.add("LP")
    for token, mod in (
        ("sff", "SFF"),
        ("v2", "V2"),
        ("plus", "Plus"),
        ("evo", "EVO"),
        ("itx", "ITX"),
    ):
        if token in toks:
            mods.add(mod)
    if re.search(r"\bv 2\b", text):
        mods.add("V2")
    return mods


def _memtype(text: str) -> str | None:
    m = _MEMTYPE.search(text)
    if not m:
        return None
    gen = int(m.group(2))
    return f"GDDR{gen}" if gen >= 5 else f"DDR{gen}"


def parse_gpu(title: str, store_brand: str | None, tax: Taxonomy) -> Parsed | ParseFailure:
    text = spaced(title)
    chip = _pick_chip(_find_chips(text))
    if isinstance(chip, ParseFailure):
        return chip

    notes: list[str] = []
    allowed = tax.gpu_chips.get(chip.name)
    if allowed is None:
        notes.append("unknown_chip")
    vram = _vram(text, allowed)
    if vram is None and allowed and len(allowed) == 1:
        vram = allowed[0]
        notes.append("vram_inferred")
    if vram is None:
        return ParseFailure("vram_missing")

    family_name = f"{chip.name} {vram}GB"
    family_id = f"gpu:{slug(family_name)}"
    vendor = (
        "NVIDIA"
        if chip.base.startswith(("RTX", "GTX", "GT"))
        else ("AMD" if chip.base.startswith("RX") else "Intel")
    )
    series = {"NVIDIA": "GeForce", "AMD": "Radeon", "Intel": None}[vendor]

    known_brands = set(tax.gpu_lines) | tax.gpu_lineless_brands | GPU_BRANDS_EXTRA
    brand = tax.detect_brand(title, allowed=known_brands)
    if brand is None and store_brand in known_brands:
        brand = store_brand

    line = match_alias(tax.gpu_lines[brand], title) if brand in tax.gpu_lines else None
    mods = _modifiers(title, text)
    if chip.name in tax.gpu_memtype_matters:
        memtype = _memtype(text)
        if memtype:
            mods.add(memtype)
    if line == "LP" or line == "Low Profile":
        mods.discard("LP")
    if line == "White":
        mods.discard("White")

    ordered = [m for m in _MOD_ORDER if m in mods] + sorted(mods - set(_MOD_ORDER))
    name = join_name(brand, series, chip.name, f"{vram}GB", line, *ordered)

    fingerprint = None
    if brand and (line or brand in tax.gpu_lineless_brands):
        fingerprint = "|".join(
            [
                "gpu",
                brand.lower(),
                chip.name.lower(),
                str(vram),
                (line or "").lower(),
                "+".join(sorted(m.lower() for m in mods)),
            ]
        )
    elif brand is None:
        notes.append("unknown_brand")
    else:
        notes.append("unknown_line")

    specs = {"vendor": vendor, "chip": chip.name, "vram_gb": vram}
    return Parsed(
        category=Category.GPU,
        family_id=family_id,
        family_name=family_name,
        name=name,
        brand=brand,
        line=line,
        fingerprint=fingerprint,
        specs={**specs, "modifiers": ordered},
        family_specs=specs,
        notes=notes,
    )
