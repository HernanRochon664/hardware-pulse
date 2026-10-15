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
from src.entities.text import compact, fold
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
    )


@lru_cache(maxsize=4)
def load_taxonomy(path: Path | None = None) -> Taxonomy:
    path = path or TAXONOMY_PATH
    return _load(yaml.safe_load(path.read_text(encoding="utf-8")))
