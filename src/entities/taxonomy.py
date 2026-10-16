"""
Loader and matchers for ``configs/taxonomy.yaml``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from src.domain.models import Category, Condition
from src.entities.text import compact, fold, spaced
from src.settings import TAXONOMY_PATH

_TOKEN_SPLIT = re.compile(r"[^a-z0-9+_.]+")


def fold_tokens(text: str) -> list[str]:
    """Tokens of the folded text split only on separators ("x3" stays "x3")."""
    return [t for t in _TOKEN_SPLIT.split(fold(text)) if t]


def _phrase_regex(phrase: str) -> re.Pattern[str]:
    return re.compile(r"(?<![a-z0-9])" + re.escape(fold(phrase)) + r"(?![a-z0-9])")


@dataclass(frozen=True)
class Alias:
    canonical: str
    key: str  # compact form, or the folded token for short aliases
    short: bool

    @property
    def weight(self) -> int:
        return len(self.key)


def _aliases(table: dict[str, list[str]]) -> list[Alias]:
    out: list[Alias] = []
    for canonical, names in table.items():
        canonical = str(canonical)  # YAML reads model names like 980 as int
        for name in map(str, [canonical, *names]):
            key = compact(name)
            if not key:
                continue
            short = len(key) <= 2
            out.append(Alias(canonical, fold(name).strip() if short else key, short))
    # Longest alias first so "gaming trio" beats "gaming".
    return sorted(out, key=lambda a: a.weight, reverse=True)


def match_alias(aliases: list[Alias], title: str) -> str | None:
    """Return the canonical name of the longest alias present in the title."""
    packed = compact(title)
    toks = set(fold_tokens(title))
    for alias in aliases:
        if (alias.key in toks) if alias.short else (alias.key in packed):
            return alias.canonical
    return None


@dataclass(frozen=True)
class SsdModel:
    name: str
    iface: str


# ---------------------------------------------------------------------------
# Model tables of the catalog expansion (coolers, PSUs, cases, monitor lines).
# An alias may hold a "{n}" placeholder for a number that is part of the model
# code: "ud{n}gm" reads UD750GM as series "UD GM" + 750 (watts), "le{n} v2" reads
# LE360 V2 as "LE V2" + 360 (radiator mm). Plain aliases are matched on the
# compact title (like the other tables); placeholder aliases on the spaced title,
# so "Atlas 600 80 Plus" reads 600 and not 6008.
# ---------------------------------------------------------------------------

_NUMBER = "(?P<n>[0-9]{3,4})"


@dataclass(frozen=True)
class ModelAlias:
    canonical: str
    attrs: dict[str, Any]
    regex: re.Pattern[str] | None  # None for short aliases
    token: str | None  # short aliases (<= 2 chars) must be whole tokens
    weight: int
    spaced: bool = False  # regex runs over spaced() instead of compact()


def _model_aliases(table: dict[str, Any]) -> list[ModelAlias]:
    out: list[ModelAlias] = []
    for canonical, spec in table.items():
        canonical = str(canonical)
        spec = spec or {}
        names = [canonical, *map(str, spec.get("aliases", []))]
        attrs = {k: v for k, v in spec.items() if k != "aliases"}
        for name in names:
            parts = name.split("{n}")
            literal = "".join(compact(p) for p in parts)
            if not literal:
                continue
            if len(parts) == 1:
                if len(literal) <= 2:
                    out.append(ModelAlias(canonical, attrs, None, fold(name).strip(), len(literal)))
                else:
                    out.append(
                        ModelAlias(
                            canonical, attrs, re.compile(re.escape(literal)), None, len(literal)
                        )
                    )
                continue
            pieces: list[str] = []
            for i, part in enumerate(parts):
                if spaced(part):
                    pieces.append(re.escape(spaced(part)))
                if i < len(parts) - 1:
                    pieces.append(_NUMBER)
            regex = re.compile(r"(?<![a-z0-9])" + " ?".join(pieces) + r"(?![a-z0-9])")
            out.append(ModelAlias(canonical, attrs, regex, None, len(literal), spaced=True))
    # Longest first; with the same literal length, the alias that reads a number wins.
    return sorted(out, key=lambda a: (a.weight, a.spaced), reverse=True)


def match_model(
    aliases: list[ModelAlias], title: str
) -> tuple[str, dict[str, Any], int | None] | None:
    """Longest model alias in the title: (canonical name, attributes, placeholder number)."""
    packed = compact(title)
    text = spaced(title)
    toks = set(fold_tokens(title))
    for alias in aliases:
        if alias.regex is None:
            if alias.token in toks:
                return alias.canonical, alias.attrs, None
            continue
        m = alias.regex.search(text if alias.spaced else packed)
        if m:
            number = m.groupdict().get("n")
            return alias.canonical, alias.attrs, int(number) if number else None
    return None


@dataclass
class Taxonomy:
    conditions: dict[Condition, list[re.Pattern[str]]]
    rejects: dict[Category, list[re.Pattern[str]]]
    brands: list[tuple[tuple[str, ...], str]]
    gpu_lineless_brands: set[str]
    gpu_lines: dict[str, list[Alias]]
    gpu_chips: dict[str, list[int]]
    gpu_memtype_matters: set[str]
    ram_lines: dict[str, list[Alias]]
    ram_speeds: set[int]
    ssd_models: dict[str, list[Alias]]
    ssd_ifaces: dict[tuple[str, str], str] = field(default_factory=dict)
    # Catalog expansion (motherboard, cooler, psu, case, monitor).
    category_brands: dict[Category, set[str]] = field(default_factory=dict)
    board_chipsets: dict[str, tuple[str, str | None]] = field(default_factory=dict)
    board_words: dict[str, str] = field(default_factory=dict)
    board_noise: set[str] = field(default_factory=set)
    cooler_models: dict[str, list[ModelAlias]] = field(default_factory=dict)
    psu_series: dict[str, list[ModelAlias]] = field(default_factory=dict)
    case_models: dict[str, list[ModelAlias]] = field(default_factory=dict)
    monitor_lines: dict[str, list[ModelAlias]] = field(default_factory=dict)

    def brands_for(self, category: Category, *tables: dict[str, Any]) -> set[str]:
        """Brands accepted for a category: its brand list plus the keys of its model tables."""
        allowed = set(self.category_brands.get(category, set()))
        for table in tables:
            allowed |= set(table)
        return allowed

    # -- brand -----------------------------------------------------------------

    def detect_brand(self, title: str, allowed: set[str] | None = None) -> str | None:
        """Earliest brand mentioned in the title (multi-word aliases supported)."""
        toks = fold_tokens(title)
        best: tuple[int, int, str] | None = None
        for alias, brand in self.brands:
            if allowed is not None and brand not in allowed:
                continue
            n = len(alias)
            for i in range(len(toks) - n + 1):
                if tuple(toks[i : i + n]) == alias:
                    candidate = (i, -n, brand)
                    if best is None or candidate < best:
                        best = candidate
                    break
        return best[2] if best else None

    def normalize_brand(self, value: str | None) -> str | None:
        """Map a store-provided brand ("ASROCK", "OTRAS MARCAS") to a known brand."""
        if not value:
            return None
        return self.detect_brand(value)

    # -- condition / rejects ----------------------------------------------------

    def condition(self, title: str) -> Condition:
        folded = fold(title)
        for condition, patterns in self.conditions.items():
            if any(p.search(folded) for p in patterns):
                return condition
        return Condition.NEW

    def reject_reason(self, category: Category, title: str) -> str | None:
        folded = fold(title)
        for pattern in self.rejects.get(category, []):
            if pattern.search(folded):
                return pattern.pattern
        return None


def _load(raw: dict[str, Any]) -> Taxonomy:
    brands = [
        (tuple(fold_tokens(alias)), name)
        for name, aliases in raw["brands"].items()
        for alias in [name, *aliases]
    ]
    ssd_models: dict[str, list[Alias]] = {}
    ssd_ifaces: dict[tuple[str, str], str] = {}
    for brand, models in raw.get("ssd_models", {}).items():
        table = {str(name): spec["aliases"] for name, spec in models.items()}
        ssd_models[brand] = _aliases(table)
        for name, spec in models.items():
            ssd_ifaces[(brand, str(name))] = spec["iface"]
    return Taxonomy(
        conditions={
            Condition(name): [_phrase_regex(p) for p in phrases]
            for name, phrases in raw["conditions"].items()
        },
        rejects={
            Category(name): [_phrase_regex(p) for p in phrases]
            for name, phrases in raw["rejects"].items()
        },
        brands=brands,
        gpu_lineless_brands=set(raw.get("gpu_lineless_brands", [])),
        gpu_lines={b: _aliases(lines) for b, lines in raw.get("gpu_lines", {}).items()},
        gpu_chips={chip: list(vram) for chip, vram in raw.get("gpu_chips", {}).items()},
        gpu_memtype_matters=set(raw.get("gpu_memtype_matters", [])),
        ram_lines={b: _aliases(lines) for b, lines in raw.get("ram_lines", {}).items()},
        ram_speeds=set(raw.get("ram_speeds", [])),
        ssd_models=ssd_models,
        ssd_ifaces=ssd_ifaces,
        category_brands={
            Category(name): set(brands) for name, brands in raw.get("category_brands", {}).items()
        },
        board_chipsets={
            str(chipset): (str(spec[0]), spec[1])
            for chipset, spec in raw.get("board_chipsets", {}).items()
        },
        board_words={
            fold(str(alias)): str(canonical)
            for canonical, aliases in raw.get("board_words", {}).items()
            for alias in [canonical, *(aliases or [])]
        },
        board_noise={fold(str(w)) for w in raw.get("board_noise", [])},
        cooler_models={b: _model_aliases(t) for b, t in raw.get("cooler_models", {}).items()},
        psu_series={b: _model_aliases(t) for b, t in raw.get("psu_series", {}).items()},
        case_models={b: _model_aliases(t) for b, t in raw.get("case_models", {}).items()},
        monitor_lines={b: _model_aliases(t) for b, t in raw.get("monitor_lines", {}).items()},
    )


@lru_cache(maxsize=4)
def load_taxonomy(path: Path | None = None) -> Taxonomy:
    path = path or TAXONOMY_PATH
    return _load(yaml.safe_load(path.read_text(encoding="utf-8")))
