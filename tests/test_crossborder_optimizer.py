"""
Cart optimizer: the subset dynamic programming must match an independent brute
force (every assignment of units to local / shipments / regimes / people), and
the plans must respect every rule.
"""

from __future__ import annotations

import math
import random
from dataclasses import replace
from datetime import date

import pytest

from src.crossborder.config import CrossBorderConfig, load_config
from src.crossborder.engine import (
    ForeignItem,
    FranchisePool,
    Regime,
    Settings,
    ShipmentQuote,
    quote_shipment,
    store_lines,
)
from src.crossborder.optimizer import MAX_UNITS, CartItem, Unit, expand_units, optimize
from tests.test_crossborder_engine import AERO, GRIP

SETTINGS = Settings(arrival=date(2026, 11, 20), courier_id="grip")


@pytest.fixture(scope="module")
def cfg() -> CrossBorderConfig:
    real = load_config()
    return CrossBorderConfig(real.rules, {"grip": GRIP, "aero": AERO}, real.stores, real.profiles)


def brute_force(
    units: list[Unit],
    pools: list[FranchisePool],
    cfg: CrossBorderConfig,
    hassle_usd: float,
) -> tuple[float, int]:
    """Min (cents, shipments) over every assignment, with the optimizer's cost conventions."""
    courier = cfg.couriers[SETTINGS.courier_id or "grip"]
    hassle = round(hassle_usd * 100)
    cache: dict[tuple[int, Regime], float] = {}

    def shipment_cost(mask: int, regime: Regime) -> float:
        key = (mask, regime)
        if key not in cache:
            items = [units[i].foreign for i in range(len(units)) if mask >> i & 1]
            quote = quote_shipment(items, regime, SETTINGS, courier, cfg)  # type: ignore[arg-type]
            cache[key] = (
                round(quote.total * 100) + hassle if isinstance(quote, ShipmentQuote) else math.inf
            )
        return cache[key]

    def invoice(mask: int) -> float:
        items = [units[i].foreign for i in range(len(units)) if mask >> i & 1]
        return store_lines(items, SETTINGS, courier, cfg)[1]  # type: ignore[arg-type]

    best = (math.inf, 0)
    n = len(units)

    def assign(i: int, labels: list[int], blocks: int) -> None:
        nonlocal best
        if i == n:
            masks = [0] * blocks
            local = 0
            for unit_index, label in enumerate(labels):
                if label < 0:
                    local += round((units[unit_index].item.local_usd or 0) * 100)
                else:
                    masks[label] |= 1 << unit_index
            # Each block: 60% (-1) or the franchise of person p.
            for choice in _product(range(-1, len(pools)), blocks):
                cost = local
                by_person: dict[int, list[int]] = {}
                for mask, who in zip(masks, choice, strict=True):
                    regime = Regime.SINGLE_TAX if who < 0 else Regime.FRANCHISE
                    cost += shipment_cost(mask, regime)
                    if who >= 0:
                        by_person.setdefault(who, []).append(mask)
                if cost == math.inf:
                    continue
                if any(
                    len(ms) > pools[p].shipments_left
                    or invoice(_union(ms)) > pools[p].usd_left + 1e-9
                    for p, ms in by_person.items()
                ):
                    continue
                best = min(best, (cost, blocks))
            return
        unit = units[i]
        if unit.item.local_usd is not None:
            assign(i + 1, [*labels, -1], blocks)
        if unit.foreign is not None:
            for b in range(blocks + 1):
                assign(i + 1, [*labels, b], max(blocks, b + 1))

    assign(0, [], 0)
    return best


def _product(values: range, repeat: int):
    if repeat == 0:
        yield ()
        return
    for head in values:
        for tail in _product(values, repeat - 1):
            yield (head, *tail)


def _union(masks: list[int]) -> int:
    out = 0
    for mask in masks:
        out |= mask
    return out


def _objective(plan, hassle_usd: float) -> tuple[float, int]:
    cents = round(plan.local_total * 100) + sum(
        round(s.quote.total * 100) + round(hassle_usd * 100) for s in plan.shipments
    )
    return (cents, len(plan.shipments))


def _random_cart(rng: random.Random, n: int) -> list[CartItem]:
    items = []
    for i in range(n):
        price = round(rng.uniform(20, 420), 2)
        store = rng.choice(["amazon", "amazon", "amazon_marketplace", "newegg"])
        foreign = ForeignItem(
            f"P{i}", price, store_id=store, unit_weight_kg=round(rng.uniform(0.05, 3.0), 2)
        )
        local = round(price * rng.uniform(1.0, 1.8), 2) if rng.random() < 0.85 else None
        if rng.random() < 0.1:
            foreign = None  # only sold here
            local = local or round(price * 1.3, 2)
        items.append(CartItem(f"P{i}", 1, local, "Tienda", foreign))
    return items


def test_dp_matches_brute_force_on_random_carts(cfg):
    rng = random.Random(20261016)
    for _ in range(120):
        n = rng.randint(1, 5)
        cart = _random_cart(rng, n)
        pools = [
            FranchisePool(f"P{p}", rng.randint(0, 3), rng.choice([800.0, 450.0, 150.0]))
            for p in range(rng.randint(0, 2))
        ]
        hassle = rng.choice([0.0, 0.0, 7.5, 40.0])
        result = optimize(cart, pools, SETTINGS, cfg, hassle_usd=hassle)
        units = [u for u in expand_units(cart) if u not in result.unplaced]
        expected = brute_force(units, pools, cfg, hassle)
        if expected[0] == math.inf:
            assert result.best is None
            continue
        assert result.best is not None
        assert _objective(result.best, hassle) == expected


def test_plans_respect_every_rule(cfg):
    rng = random.Random(7)
    for _ in range(60):
        cart = _random_cart(rng, rng.randint(2, 6))
        pools = [FranchisePool("Vos", 3, 800.0), FranchisePool("Mamá", 2, 500.0)]
        plan = optimize(cart, pools, SETTINGS, cfg).best
        assert plan is not None
        used: dict[str, list[ShipmentQuote]] = {}
        for shipment in plan.shipments:
            assert shipment.quote.invoice_usd <= 800
            assert shipment.quote.real_kg <= 20
            assert (shipment.holder is None) == (shipment.quote.regime is Regime.SINGLE_TAX)
            if shipment.holder:
                used.setdefault(shipment.holder, []).append(shipment.quote)
        for pool in pools:
            quotes = used.get(pool.holder, [])
            assert len(quotes) <= pool.shipments_left
            assert sum(q.invoice_usd for q in quotes) <= pool.usd_left + 0.05
        placed = len(plan.local) + sum(len(s.quote.items) for s in plan.shipments)
        assert placed == len(expand_units(cart))


def test_three_exempt_shipments_beat_one_consolidated_unless_shipping_is_a_hassle(cfg):
    ram = ForeignItem("RAM", 150, unit_weight_kg=0.18)
    cart = [CartItem("RAM", 3, 260, "Laaca", ram)]
    pools = [FranchisePool()]
    split = optimize(cart, pools, SETTINGS, cfg).best
    assert split is not None
    assert len(split.shipments) == 3
    assert all(s.quote.exempt for s in split.shipments)  # 160.50 each, under USD 200
    consolidated = optimize(cart, pools, SETTINGS, cfg, hassle_usd=60).best
    assert consolidated is not None
    assert len(consolidated.shipments) < 3


def test_relatives_franchises_add_exempt_shipments(cfg):
    # Four USD 160.50 invoices: each one is exempt alone, so the best plan uses
    # four franchise shipments, two of each person.
    ram = ForeignItem("RAM", 150, unit_weight_kg=0.18)
    cart = [CartItem("RAM", 4, 260, "Laaca", ram)]
    pools = [FranchisePool("Vos", 2, 800), FranchisePool("Mamá", 2, 800)]
    plan = optimize(cart, pools, SETTINGS, cfg).best
    assert plan is not None
    assert sorted(s.holder or "" for s in plan.shipments) == ["Mamá", "Mamá", "Vos", "Vos"]
    assert all(s.quote.exempt for s in plan.shipments)
    alone = optimize(cart, pools[:1], SETTINGS, cfg).best
    assert alone is not None
    assert alone.total > plan.total


def test_an_item_over_800_is_bought_here_or_reported(cfg):
    gpu = ForeignItem("RTX 5090", 1999, unit_weight_kg=3.8)
    with_local = optimize(
        [CartItem("RTX 5090", 1, 3100, "Thot", gpu)], [FranchisePool()], SETTINGS, cfg
    )
    assert with_local.best is not None
    assert [u.item.label for u in with_local.best.local] == ["RTX 5090"]
    assert with_local.best.shipments == ()
    only_abroad = optimize(
        [CartItem("RTX 5090", 1, None, None, gpu)], [FranchisePool()], SETTINGS, cfg
    )
    assert [u.item.label for u in only_abroad.unplaced] == ["RTX 5090"]


def test_cheapest_courier_is_chosen_per_plan(cfg):
    cart = [CartItem("SSD", 1, 200, "PCParts", ForeignItem("SSD", 120, unit_weight_kg=0.08))]
    result = optimize(cart, [FranchisePool()], replace(SETTINGS, courier_id=None), cfg)
    assert result.best is not None
    assert {p.courier.id for p in result.by_courier} == {"grip", "aero"}
    assert result.best.objective == min(p.objective for p in result.by_courier)


def test_unit_limit():
    item = CartItem("RAM", MAX_UNITS + 1, 50, "X", ForeignItem("RAM", 30))
    with pytest.raises(ValueError, match="hasta"):
        expand_units([item])
