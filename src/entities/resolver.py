"""
Entity resolution: store title -> (condition, family, exact product).

Pipeline per listing:
1. The category comes from the store category that was scraped (never guessed).
2. Condition: open box / used / refurbished titles are not "new".
3. Rejects: accessories, hard drives, coolers... sharing the store category.
4. Category parser (strict, no fuzzy matching).
5. Identity: family always; exact product only when brand + line/model are known.

Statuses:
- matched      exact product fingerprint, shared across stores
- family_only  family known, product line unknown: own product, never merged
- rejected     not a product of this category
- unparsed     could not extract the required specs (shown in "Estado de datos")
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from src.domain.models import Category, Condition
from src.entities.mpn import extract_mpn
from src.entities.specs import Parsed, ParseFailure
from src.entities.specs.cpu import parse_cpu
from src.entities.specs.gpu import parse_gpu
from src.entities.specs.ram import parse_ram
from src.entities.specs.ssd import parse_ssd
from src.entities.taxonomy import Taxonomy, load_taxonomy

# Bump whenever parsing rules or the taxonomy change in a way that should
# re-resolve every stored listing.
RESOLVER_VERSION = "2.0.0"

Parser = Callable[[str, str | None, Taxonomy], Parsed | ParseFailure]

PARSERS: dict[Category, Parser] = {
    Category.GPU: parse_gpu,
    Category.CPU: parse_cpu,
    Category.RAM: parse_ram,
    Category.SSD: parse_ssd,
}


@dataclass
class Resolution:
    status: str
    condition: Condition
    parsed: Parsed | None = None
    mpn: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def notes_text(self) -> str:
        return "; ".join(self.notes)


def resolve_title(
    category: Category,
    title: str,
    store_brand: str | None = None,
    taxonomy: Taxonomy | None = None,
) -> Resolution:
    tax = taxonomy or load_taxonomy()
    condition = tax.condition(title)
    reason = tax.reject_reason(category, title)
    if reason:
        return Resolution("rejected", condition, notes=[f"reject:{reason}"])

    result = PARSERS[category](title, tax.normalize_brand(store_brand), tax)
    if isinstance(result, ParseFailure):
        # "not_*" failures mean the item is not this kind of product at all.
        status = "rejected" if result.reason.startswith("not_") else "unparsed"
        return Resolution(status, condition, notes=[result.reason])

    status = "matched" if result.fingerprint else "family_only"
    return Resolution(
        status,
        condition,
        parsed=result,
        mpn=extract_mpn(title),
        notes=list(result.notes),
    )
