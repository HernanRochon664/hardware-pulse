"""
Several products at once ("Armado"): what to buy in Uruguay, what to import,
how to group the imports into shipments and whose franchise each one uses.

The search is exact. For each courier (a plan uses one courier account), over
the subsets ("masks") of units:

- ``single[m]`` / ``franchise[m]``: the units of ``m`` as one shipment under the
  60% regime / the franchise (taxes, freight, fees), plus a per-shipment hassle
  cost; infinite when not eligible (> USD 800, > 20 kg...);
- ``rest[m]``: cheapest way to cover ``m`` with local purchases and 60% shipments;
- ``q[k][m]``: cheapest split of ``m`` into at most ``k`` franchise shipments.
  The shipments of one person partition their mask, so that person's yearly
  cap is checked on the invoice total of the whole mask;
- people are added one at a time: ``H_p[m] = min over sub ⊆ m of
  q[k_p][sub] + H_{p-1}[m \\ sub]``.

Costs are integer cents paired with the number of shipments, so equal totals
prefer fewer shipments. Each pass is O(3^n); n is capped at MAX_UNITS.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace

from src.crossborder.config import Courier, CrossBorderConfig
from src.crossborder.engine import (
    ForeignItem,
    FranchisePool,
    Regime,
    Settings,
    ShipmentQuote,
    compare,
    couriers_for,
    quote_shipment,
    store_lines,
)
from src.crossborder.money import cents, usd

MAX_UNITS = 10

Cost = tuple[float, int]  # (cents, shipments)
_INF: Cost = (math.inf, 0)
_ZERO: Cost = (0, 0)


@dataclass(frozen=True)
class CartItem:
    """A product of the cart: its best local offer and/or its US offer (per unit)."""

    label: str
    qty: int = 1
    local_usd: float | None = None
    local_store: str | None = None
    foreign: ForeignItem | None = None

    def __post_init__(self) -> None:
        if self.qty < 1:
            raise ValueError(f"{self.label}: quantity must be positive")


@dataclass(frozen=True)
class Unit:
    item: CartItem
    foreign: ForeignItem | None


@dataclass(frozen=True)
class PlannedShipment:
    quote: ShipmentQuote
    holder: str | None  # whose franchise it uses; None for the 60% regime

    @property
    def units_label(self) -> str:
        return " + ".join(item.label for item in self.quote.items)


@dataclass(frozen=True)
class Plan:
    courier: Courier
    local: tuple[Unit, ...]
    shipments: tuple[PlannedShipment, ...]
    total: float
    hassle_usd: float
    notes: tuple[str, ...] = ()

    @property
    def objective(self) -> float:
        return cents(self.total + self.hassle_usd * len(self.shipments))

    @property
    def local_total(self) -> float:
        return cents(sum(unit.item.local_usd or 0.0 for unit in self.local))


@dataclass(frozen=True)
class CartResult:
    best: Plan | None
    by_courier: tuple[Plan, ...]
    unplaced: tuple[Unit, ...]
    all_local: float | None
    one_shipment: ShipmentQuote | None


def expand_units(items: Sequence[CartItem]) -> list[Unit]:
    units = [
        Unit(item, replace(item.foreign, qty=1) if item.foreign else None)
        for item in items
        for _ in range(item.qty)
    ]
    if len(units) > MAX_UNITS:
        raise ValueError(f"El armado admite hasta {MAX_UNITS} unidades.")
    return units


def optimize(
    items: Sequence[CartItem],
    pools: Sequence[FranchisePool],
    settings: Settings,
    cfg: CrossBorderConfig,
    *,
    hassle_usd: float = 0.0,
) -> CartResult:
    """Best plan across couriers; units that can be neither bought here nor imported are listed."""
    units = expand_units(items)
    couriers = couriers_for(settings, cfg)
    placeable: list[Unit] = []
    unplaced: list[Unit] = []
    for unit in units:
        if unit.item.local_usd is not None or _importable_alone(unit, settings, cfg, couriers):
            placeable.append(unit)
        else:
            unplaced.append(unit)

    plans = [
        plan
        for courier in couriers
        if (plan := _optimize_for_courier(placeable, pools, settings, cfg, courier, hassle_usd))
    ]
    plans.sort(key=lambda p: (p.objective, len(p.shipments), p.courier.name))

    all_local = None
    if all(unit.item.local_usd is not None for unit in units):
        all_local = cents(sum(unit.item.local_usd or 0.0 for unit in units))
    foreign = [unit.foreign for unit in units if unit.foreign is not None]
    one_shipment = None
    if foreign and len(foreign) == len(units):
        one_shipment = compare(foreign, settings, cfg, pools[0] if pools else None).best
    return CartResult(
        plans[0] if plans else None, tuple(plans), tuple(unplaced), all_local, one_shipment
    )


def _importable_alone(
    unit: Unit, settings: Settings, cfg: CrossBorderConfig, couriers: list[Courier]
) -> bool:
    foreign = unit.foreign
    if foreign is None:
        return False
    quotes = (quote_shipment([foreign], Regime.SINGLE_TAX, settings, c, cfg) for c in couriers)
    return any(isinstance(quote, ShipmentQuote) for quote in quotes)


def _bits(mask: int) -> list[int]:
    return [i for i in range(mask.bit_length()) if mask >> i & 1]


def _add(a: Cost, b: Cost) -> Cost:
    return (a[0] + b[0], a[1] + b[1])


def _optimize_for_courier(
    units: Sequence[Unit],
    pools: Sequence[FranchisePool],
    settings: Settings,
    cfg: CrossBorderConfig,
    courier: Courier,
    hassle_usd: float,
) -> Plan | None:
    n = len(units)
    size = 1 << n
    full = size - 1
    hassle = round(hassle_usd * 100)
    importable = sum(1 << i for i, unit in enumerate(units) if unit.foreign is not None)
    local = [
        round(unit.item.local_usd * 100) if unit.item.local_usd is not None else None
        for unit in units
    ]

    # One shipment per mask, under each regime.
    single: list[Cost] = [_INF] * size
    franchise: list[Cost] = [_INF] * size
    single_quote: list[ShipmentQuote | None] = [None] * size
    franchise_quote: list[ShipmentQuote | None] = [None] * size
    value = [math.inf] * size
    value[0] = 0.0
    for m in range(1, size):
        if m & ~importable:
            continue
        shipment = [units[i].foreign for i in _bits(m)]
        items = [item for item in shipment if item is not None]
        value[m] = store_lines(items, settings, courier, cfg)[1]
        for regime, costs, quotes in (
            (Regime.SINGLE_TAX, single, single_quote),
            (Regime.FRANCHISE, franchise, franchise_quote),
        ):
            quote = quote_shipment(items, regime, settings, courier, cfg)
            if isinstance(quote, ShipmentQuote):
                costs[m] = (round(quote.total * 100) + hassle, 1)
                quotes[m] = quote

    # rest[m]: local purchases and 60% shipments.
    rest: list[Cost] = [_INF] * size
    rest_pick = [0] * size
    rest_local = [False] * size
    rest[0] = _ZERO
    for m in range(1, size):
        low = m & -m
        best, pick, is_local = _INF, 0, False
        price = local[low.bit_length() - 1]
        if price is not None and rest[m ^ low][0] < math.inf:
            best, pick, is_local = _add((price, 0), rest[m ^ low]), low, True
        sub = m
        while sub:
            if sub & low and single[sub][0] < math.inf and rest[m ^ sub][0] < math.inf:
                candidate = _add(single[sub], rest[m ^ sub])
                if candidate < best:
                    best, pick, is_local = candidate, sub, False
            sub = (sub - 1) & m
        rest[m], rest_pick[m], rest_local[m] = best, pick, is_local

    # q[k][m]: at most k franchise shipments covering m.
    max_k = min(max((p.shipments_left for p in pools), default=0), 3)
    q: list[list[Cost]] = [[_ZERO] + [_INF] * (size - 1) for _ in range(max_k + 1)]
    q_pick = [[0] * size for _ in range(max_k + 1)]
    for k in range(1, max_k + 1):
        previous, current, pick = q[k - 1], q[k], q_pick[k]
        for m in range(1, size):
            if m & ~importable:
                continue
            best, choice = previous[m], 0
            low = m & -m
            sub = m
            while sub:
                if sub & low and franchise[sub][0] < math.inf and previous[m ^ sub][0] < math.inf:
                    candidate = _add(franchise[sub], previous[m ^ sub])
                    if candidate < best:
                        best, choice = candidate, sub
                sub = (sub - 1) & m
            current[m], pick[m] = best, choice

    # People, one at a time.
    layers: list[list[int]] = []
    totals = rest
    for pool in pools:
        k = min(max(pool.shipments_left, 0), max_k)
        cover = q[k]
        layer_totals: list[Cost] = [_INF] * size
        layer_pick = [0] * size
        for m in range(size):
            best, choice = totals[m], 0
            sub = m
            while sub:
                if (
                    cover[sub][0] < math.inf
                    and value[sub] <= pool.usd_left + 1e-9
                    and totals[m ^ sub][0] < math.inf
                ):
                    candidate = _add(cover[sub], totals[m ^ sub])
                    if candidate < best:
                        best, choice = candidate, sub
                sub = (sub - 1) & m
            layer_totals[m], layer_pick[m] = best, choice
        layers.append(layer_pick)
        totals = layer_totals

    if totals[full][0] == math.inf:
        return None

    # Rebuild the plan.
    shipments: list[PlannedShipment] = []
    notes: list[str] = []
    m = full
    for pool, layer_pick in zip(reversed(pools), reversed(layers), strict=True):
        sub = layer_pick[m]
        if not sub:
            continue
        k = min(max(pool.shipments_left, 0), max_k)
        quotes = [franchise_quote[block] for block in _split(q_pick, k, sub)]
        used = 0.0
        for quote in quotes:
            assert quote is not None
            shipments.append(PlannedShipment(quote, pool.holder))
            used += quote.invoice_usd
        # The cap was checked on one invoice for the whole mask; separate invoices
        # can differ by a cent of sales tax rounding.
        if cents(used) > pool.usd_left:
            notes.append(
                f"La franquicia de {pool.holder} queda justa ({usd(used)} de "
                f"{usd(pool.usd_left)}): verificá los totales exactos de las facturas."
            )
        m ^= sub

    local_units: list[Unit] = []
    while m:
        pick = rest_pick[m]
        if rest_local[m]:
            local_units.append(units[pick.bit_length() - 1])
        else:
            quote = single_quote[pick]
            assert quote is not None
            shipments.append(PlannedShipment(quote, None))
        m ^= pick

    total = cents(
        sum(unit.item.local_usd or 0.0 for unit in local_units)
        + sum(s.quote.total for s in shipments)
    )
    return Plan(courier, tuple(local_units), tuple(shipments), total, hassle_usd, tuple(notes))


def _split(q_pick: list[list[int]], k: int, mask: int) -> list[int]:
    """The franchise shipments chosen by q[k][mask]."""
    blocks: list[int] = []
    while mask:
        if k <= 0:
            raise RuntimeError("inconsistent franchise split")
        choice = q_pick[k][mask]
        k -= 1
        if choice:
            blocks.append(choice)
            mask ^= choice
    return blocks
