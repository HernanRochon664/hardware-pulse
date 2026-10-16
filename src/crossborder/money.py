"""
Cost lines, cent rounding and the Uruguayan number format used in their details.

Every line is rounded to cents (half up, like an invoice) and totals are the
sum of the rounded lines, so a breakdown always adds up to its total.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

LineGroup = Literal["store", "card", "courier", "tax"]

GROUP_LABELS: dict[str, str] = {
    "store": "Tienda",
    "card": "Tarjeta",
    "courier": "Courier",
    "tax": "Impuestos en Uruguay",
}


@dataclass(frozen=True)
class CostLine:
    key: str
    label: str
    amount: float
    detail: str = ""
    source: str | None = None
    group: LineGroup = "store"


def cents(value: float) -> float:
    """Round to cents, half up (2.675 -> 2.68), using the shortest decimal repr."""
    return float(Decimal(repr(float(value))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def total(lines: list[CostLine] | tuple[CostLine, ...]) -> float:
    return cents(sum(line.amount for line in lines))


def _group(value: float, decimals: int) -> str:
    text = f"{value:,.{decimals}f}"  # 1,049.50
    return text.replace(",", "§").replace(".", ",").replace("§", ".")  # 1.049,50


def usd(value: float) -> str:
    """'U$S 1.049' or 'U$S 1.049,50' (same style as the dashboard)."""
    rounded = cents(value)
    decimals = 0 if abs(rounded - round(rounded)) < 0.005 else 2
    return f"U$S {_group(rounded, decimals)}"


def pct(rate: float) -> str:
    """0.22 -> '22%', 0.015 -> '1,5%'."""
    return f"{_group(rate * 100, 2).rstrip('0').rstrip(',')}%"


def kg(value: float) -> str:
    """1.3 -> '1,3 kg', 0.899 -> '899 g'."""
    if value < 1:
        return f"{round(value * 1000):d} g"
    return f"{_group(value, 3).rstrip('0').rstrip(',')} kg"
