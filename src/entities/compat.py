"""
Compatibility checks for a PC build ("armado por componente" in the import page).

They only use the specs the parsers read from store titles, so they are
warnings, never blocks: a spec the title does not state is never assumed.

- CPU socket vs motherboard socket;
- RAM generation (DDR4/DDR5) and module type (SODIMM) vs the motherboard;
- a CPU without integrated graphics in a build without a graphics card;
- motherboard form factor vs a case that states a smaller one.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from src.domain.models import Category

_FORM_SIZE = {"ITX": 0, "mATX": 1, "ATX": 2, "E-ATX": 3}
_CASE_MAX_FORM = {"Mini ITX": 0, "Micro ATX": 1}


@dataclass(frozen=True)
class BuildPart:
    category: Category
    name: str
    specs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CompatIssue:
    level: str  # "warning" (parts do not fit) | "info" (something to confirm)
    message: str


def _pairs(parts: Sequence[BuildPart], a: Category, b: Category):
    return [(x, y) for x in parts if x.category is a for y in parts if y.category is b]


def _pair_issues(cpu_or_ram: BuildPart, board: BuildPart) -> list[CompatIssue]:
    issues: list[CompatIssue] = []
    if cpu_or_ram.category is Category.CPU:
        cpu_socket, board_socket = cpu_or_ram.specs.get("socket"), board.specs.get("socket")
        if cpu_socket and board_socket and cpu_socket != board_socket:
            issues.append(
                CompatIssue(
                    "warning",
                    f"{cpu_or_ram.name} es {cpu_socket} y {board.name} es {board_socket}: "
                    "no son compatibles.",
                )
            )
    elif cpu_or_ram.category is Category.RAM:
        generation, memory = cpu_or_ram.specs.get("generation"), board.specs.get("memory")
        if generation and memory and generation != memory:
            issues.append(
                CompatIssue(
                    "warning", f"{cpu_or_ram.name} es {generation} y {board.name} usa {memory}."
                )
            )
        elif generation and memory is None and board.specs.get("socket") == "LGA1700":
            issues.append(
                CompatIssue(
                    "info",
                    f"{board.name} existe en versión DDR4 y DDR5 y el título no dice cuál es: "
                    f"confirmalo antes de comprar {cpu_or_ram.name} ({generation}).",
                )
            )
    return issues


def check_build(parts: Sequence[BuildPart]) -> list[CompatIssue]:
    issues: list[CompatIssue] = []
    for cpu, board in _pairs(parts, Category.CPU, Category.MOTHERBOARD):
        issues += _pair_issues(cpu, board)
    for ram, board in _pairs(parts, Category.RAM, Category.MOTHERBOARD):
        issues += _pair_issues(ram, board)
    for ram in (p for p in parts if p.category is Category.RAM):
        if ram.specs.get("form") == "SODIMM":
            issues.append(
                CompatIssue(
                    "warning",
                    f"{ram.name} es SODIMM (memoria de notebook): un mother de escritorio "
                    "usa DIMM.",
                )
            )
    has_gpu = any(p.category is Category.GPU for p in parts)
    for cpu in (p for p in parts if p.category is Category.CPU):
        if cpu.specs.get("igpu") is False and not has_gpu:
            issues.append(
                CompatIssue(
                    "info",
                    f"{cpu.name} no tiene video integrado: el armado necesita una tarjeta "
                    "de video.",
                )
            )
    for board, case in _pairs(parts, Category.MOTHERBOARD, Category.CASE):
        limit = _CASE_MAX_FORM.get(case.specs.get("form") or "")
        size = _FORM_SIZE.get(board.specs.get("form") or "")
        if limit is not None and size is not None and size > limit:
            issues.append(
                CompatIssue(
                    "warning",
                    f"{board.name} es {board.specs['form']} y {case.name} es para "
                    f"{case.specs['form']}: no entra.",
                )
            )
    return issues


def fits(candidate: BuildPart, chosen: Sequence[BuildPart]) -> bool:
    """False when the candidate clashes ("warning") with a part already chosen."""
    others = [p for p in chosen if p.category is not candidate.category]
    before = {i.message for i in check_build(others) if i.level == "warning"}
    after = {i.message for i in check_build([*others, candidate]) if i.level == "warning"}
    return not (after - before)
