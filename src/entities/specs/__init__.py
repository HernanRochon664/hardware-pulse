"""
Category-specific title parsers.

Each parser returns a ``Parsed`` (family always known; ``fingerprint`` only
when the exact product can be identified) or a ``ParseFailure`` with the reason.
Parsers never guess: an ambiguous title fails instead of being assigned to
the closest-looking product.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.domain.models import Category


@dataclass
class Parsed:
    category: Category
    family_id: str
    family_name: str
    name: str
    brand: str | None = None
    line: str | None = None
    fingerprint: str | None = None
    specs: dict[str, Any] = field(default_factory=dict)
    family_specs: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass
class ParseFailure:
    reason: str


def join_name(*parts: str | None) -> str:
    return " ".join(p for p in parts if p).replace("  ", " ").strip()
