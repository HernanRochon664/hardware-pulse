"""
Landed cost of one shipment: golden cases computed by hand.

The rules and the US store registry are the real configs; the couriers are
fixed copies of published tariffs, so these numbers do not move when the real
tariffs in configs/couriers.yaml are updated.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from src.crossborder.config import (
    Courier,
    CrossBorderConfig,
    Delivery,
    Insurance,
    Volumetric,
    WeightBand,
    load_config,
)
from src.crossborder.couriers import band_freight, ceil_to_step
from src.crossborder.engine import (
    ForeignItem,
    FranchisePool,
    Ineligible,
    Regime,
    Settings,
    ShipmentQuote,
    breakeven_price,
    compare,
    exemption_price_limit,
    quote_all_in,
    quote_shipment,
)
from src.crossborder.money import cents, total

ARRIVAL = date(2026, 11, 20)  # registry already required (from 2026-11-03)

# Gripper-like: flat price up to 899 g, per kg after; TFSPU on freight + handling.
GRIP = Courier(
    id="grip",
    name="Grip",
    site="https://grip.example",
    source_url="https://grip.example/tarifas",
    source_kind="official",
    verified_at=date(2026, 10, 16),
    warehouse="Miami-Dade",
    sales_tax_rate=0.07,
    transit_days=(3, 7),
    weight_step_kg=0.1,
    bands=[
        WeightBand(up_to_kg=0.899, flat_usd=19.80),
        WeightBand(up_to_kg=5.0, per_kg_usd=21.90),
        WeightBand(up_to_kg=20.0, per_kg_usd=16.50),
    ],
    handling_usd=5.0,
    tfspu_rate=0.10,
    tfspu_on_handling=True,
    delivery=Delivery(interior_usd=4.0, interior_per_extra_kg_usd=0.5),
)
# Aerobox-like: handling + IVA, TFSPU on freight only, volume surcharge, insurance.
AERO = Courier(
    id="aero",
    name="Aero",
    site="https://aero.example",
    source_url="https://aero.example/tarifas",
    source_kind="official",
    verified_at=date(2026, 10, 16),
    warehouse="Miami-Dade",
    sales_tax_rate=0.07,
    transit_days=(3, 7),
    weight_step_kg=0.1,
    bands=[
        WeightBand(up_to_kg=0.5, flat_usd=11.99),
        WeightBand(up_to_kg=5.0, per_kg_usd=23.50),
        WeightBand(up_to_kg=20.0, per_kg_usd=17.50),
    ],
    handling_usd=5.0,
    handling_iva=True,
    tfspu_rate=0.10,
    tfspu_on_handling=False,
    volumetric=Volumetric(divisor_cm3_per_kg=5000, free_ratio=2.0, surcharge_per_kg_usd=10.0),
    insurance=Insurance(rate=0.03, min_usd=33.0),
    delivery=Delivery(montevideo_usd=5.0, interior_usd=5.0, iva=True),
)
# Tax-free certificate holder with no sales tax when the store honors it.
FREE = GRIP.model_copy(update={"id": "free", "name": "Free", "tax_free_certificate": True})


@pytest.fixture(scope="module")
def cfg() -> CrossBorderConfig:
    real = load_config()
    return CrossBorderConfig(
        rules=real.rules,
        couriers={"grip": GRIP, "aero": AERO, "free": FREE},
        stores=real.stores,
        profiles=real.profiles,
    )


def settings(**changes) -> Settings:
    return replace(Settings(arrival=ARRIVAL, courier_id="grip"), **changes)


def ram(price: float, store: str = "amazon", **changes) -> ForeignItem:
    return ForeignItem("RAM", price, store_id=store, unit_weight_kg=0.18, **changes)


def quote(items, regime=Regime.FRANCHISE, courier=GRIP, cfg=None, **settings_changes):
    result = quote_shipment(items, regime, settings(**settings_changes), courier, cfg)
    assert isinstance(result, ShipmentQuote), getattr(result, "reasons", result)
    return result


# ---------------------------------------------------------------------------
# Uruguayan taxes
# ---------------------------------------------------------------------------


def test_mef_example_198_from_registered_us_seller_is_exempt(cfg):
    # FAQ del MEF: a USD 198 purchase from the US pays no IVA and leaves USD 602.
    # 185.05 + 7% sales tax (12.95) = an invoice of 198.00.
    q = quote([ram(185.05)], cfg=cfg)
    assert q.invoice_usd == 198.00
    assert q.exempt
    assert q.uy_tax == 0.0
    assert FranchisePool().usd_left - q.invoice_usd == 602.0


def test_sales_tax_pushes_the_invoice_over_200_and_iva_is_on_the_whole_value(cfg):
    q = quote([ram(190)], cfg=cfg)
    assert q.amount("sales_tax") == 13.30
    assert q.invoice_usd == 203.30
    assert not q.exempt
    assert q.uy_tax == 44.73  # 22% of 203.30
    assert any("límite" in b for b in q.exemption_blockers)


def test_third_party_seller_pays_iva_even_under_200(cfg):
    q = quote([ram(100, store="amazon_marketplace")], cfg=cfg)
    assert q.invoice_usd == 107.00
    assert not q.exempt
    assert q.uy_tax == 23.54  # 22% of 107


def test_franchise_iva_has_a_20_dollar_minimum(cfg):
    q = quote([ram(40, store="other")], cfg=cfg)
    assert q.invoice_usd == 42.80
    assert q.uy_tax == 20.00  # 22% would be 9.42


@pytest.mark.parametrize(("price", "tax"), [(10.0, 20.0), (500.0, 300.0)])
def test_single_tax_is_60_percent_with_a_20_dollar_minimum(cfg, price, tax):
    q = quote(
        [ram(price)],
        Regime.SINGLE_TAX,
        courier=FREE,
        cfg=cfg,
        courier_id="free",
        sales_tax_exempt=True,
    )
    assert q.invoice_usd == price
    assert q.uy_tax == tax


def test_shipment_over_the_remaining_franchise_goes_entirely_to_the_60_percent(cfg):
    items = [ram(467.29)]  # invoice 500.00 with 7% sales tax
    pool = FranchisePool(shipments_left=2, usd_left=100.0)
    franchise = quote_shipment(items, Regime.FRANCHISE, settings(), GRIP, cfg, pool=pool)
    assert isinstance(franchise, Ineligible)
    assert "le quedan" in franchise.reasons[0]
    best = compare(items, settings(), cfg, pool).best
    assert best is not None
    assert best.regime is Regime.SINGLE_TAX
    assert best.invoice_usd == 500.00
    assert best.uy_tax == 300.00  # never 100 under the franchise + 400 at 60%


def test_no_franchise_shipments_left(cfg):
    pool = FranchisePool(shipments_left=0, usd_left=800.0)
    result = quote_shipment([ram(50)], Regime.FRANCHISE, settings(), GRIP, cfg, pool=pool)
    assert isinstance(result, Ineligible)


@pytest.mark.parametrize(
    ("item", "reason"),
    [
        (ForeignItem("GPU", 760, unit_weight_kg=2.0), "800"),  # 813.20 with sales tax
        (ForeignItem("Gabinete", 100, unit_weight_kg=21.0), "20 kg"),
    ],
)
def test_over_800_or_20_kg_is_not_eligible_for_any_regime(cfg, item, reason):
    for regime in Regime:
        result = quote_shipment([item], regime, settings(), GRIP, cfg)
        assert isinstance(result, Ineligible)
        assert any(reason in r for r in result.reasons)


def test_every_item_must_be_registered_for_the_exemption(cfg):
    q = quote([ram(50), ram(40, store="amazon_marketplace")], cfg=cfg)
    assert q.invoice_usd == 96.30
    assert not q.exempt
    assert q.uy_tax == 21.19


# ---------------------------------------------------------------------------
# US side: sales tax, card, assisted purchase
# ---------------------------------------------------------------------------


def test_tax_free_certificate_removes_sales_tax_only_where_the_store_honors_it(cfg):
    honored = quote([ram(190)], courier=FREE, cfg=cfg, courier_id="free", sales_tax_exempt=True)
    assert honored.amount("sales_tax") == 0.0
    assert honored.invoice_usd == 190.0
    assert honored.exempt  # back under USD 200
    not_honored = quote(
        [ram(190, store="microcenter")],
        courier=FREE,
        cfg=cfg,
        courier_id="free",
        sales_tax_exempt=True,
    )
    assert not_honored.amount("sales_tax") == 13.30


def test_card_fee_with_iva(cfg):
    q = quote([ram(100, store="other")], cfg=cfg, card_fee_rate=0.03)
    assert q.amount("card_fee") == 3.92  # 3% of 107 = 3.21, + 22% IVA


def test_assisted_purchase_fee_is_part_of_the_invoice(cfg):
    q = quote([ram(100, store="assisted")], cfg=cfg, assisted_fee_rate=0.10)
    # 100 + 7 sales tax + 10 fee = 117, registered issuer -> exempt
    assert q.invoice_usd == 117.0
    assert q.exempt


# ---------------------------------------------------------------------------
# Courier charges
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("weight", "freight", "chargeable"),
    [
        (0.899, 19.80, 0.899),  # flat band, real weight
        (0.9, 19.71, 0.9),  # 0.9 kg x 21.90
        (0.95, 21.90, 1.0),  # rounded up to 100 g
        (5.0, 109.50, 5.0),
        (5.01, 84.15, 5.1),  # 5.1 kg x 16.50: heavier but cheaper, as published
    ],
)
def test_weight_bands_and_rounding(weight, freight, chargeable):
    result = band_freight(GRIP, weight)
    assert result is not None
    assert cents(result.amount) == freight
    assert result.chargeable_kg == chargeable


def test_ceil_to_step_is_robust_to_float_noise():
    assert ceil_to_step(1.3, 0.1) == 1.3
    assert ceil_to_step(1.30001, 0.1) == 1.4
    assert ceil_to_step(0.02, 0.1) == 0.1


def test_tfspu_on_freight_and_handling(cfg):
    q = quote([ForeignItem("CPU", 100, store_id="other", unit_weight_kg=1.0)], cfg=cfg)
    assert q.amount("freight") == 21.90
    assert q.amount("handling") == 5.00
    assert q.amount("tfspu") == 2.69  # 10% of 26.90


def test_handling_with_iva_and_tfspu_on_freight_only(cfg):
    q = quote(
        [ForeignItem("CPU", 100, unit_weight_kg=1.0)], courier=AERO, cfg=cfg, courier_id="aero"
    )
    assert q.amount("freight") == 23.50
    assert q.amount("handling") == 6.10  # 5 + 22% IVA
    assert q.amount("tfspu") == 2.35


def test_volume_surcharge_above_twice_the_real_weight(cfg):
    box = ForeignItem("Monitor", 100, unit_weight_kg=1.0, unit_dims_cm=(50, 40, 30))
    q = quote([box], courier=AERO, cfg=cfg, courier_id="aero")
    # 60,000 cm3 / 5000 = 12 kg of volume; 2 kg free; 10 kg x USD 10
    assert q.amount("volume") == 100.00
    assert q.amount("tfspu") == 12.35  # 10% of (23.50 + 100)


def test_insurance_minimum(cfg):
    q = quote([ram(150)], courier=AERO, cfg=cfg, courier_id="aero", insurance=True)
    assert q.amount("insurance") == 33.00  # 3% of 160.50 is 4.82


def test_delivery_to_the_interior(cfg):
    gpu = ForeignItem("GPU", 300, unit_weight_kg=2.3)
    q = quote([gpu], cfg=cfg, delivery="interior")
    assert q.chargeable_kg == 2.3
    assert q.amount("delivery") == 5.00  # 4 + 2 extra kg x 0.50
    aero = quote([gpu], courier=AERO, cfg=cfg, courier_id="aero", delivery="montevideo")
    assert aero.amount("delivery") == 6.10  # 5 + IVA


def test_every_line_is_in_cents_and_the_total_adds_up(cfg):
    q = quote(
        [ram(123.456), ram(77.77, store="other")],
        courier=AERO,
        cfg=cfg,
        courier_id="aero",
        card_fee_rate=0.015,
        insurance=True,
        delivery="interior",
    )
    for line in q.lines:
        assert line.amount == cents(line.amount)
    assert q.total == total(q.lines)
    assert q.total == cents(sum(line.amount for line in q.lines))


# ---------------------------------------------------------------------------
# Comparison, breakeven and all-in routes
# ---------------------------------------------------------------------------


def test_compare_prefers_the_franchise_and_reports_its_value(cfg):
    comparison = compare([ram(150)], replace(settings(), courier_id=None), cfg, FranchisePool())
    best, single = comparison.best, comparison.best_single_tax
    assert best is not None and single is not None
    assert best.regime is Regime.FRANCHISE
    assert best.exempt
    assert comparison.franchise_value == cents(single.total - best.total)
    assert {q.courier.id for q in comparison.best_by_courier()} == {"grip", "aero", "free"}


def test_breakeven_is_the_last_cent_that_meets_the_target(cfg):
    item = ForeignItem("GPU", 500, unit_weight_kg=2.0)
    pool = FranchisePool()
    target = 615.0
    price = breakeven_price(item, target, settings(), cfg, pool)
    assert price is not None

    def cost(p: float) -> float:
        best = compare([replace(item, unit_price_usd=p)], settings(), cfg, pool).best
        assert best is not None
        return best.total

    assert cost(price) <= target < cost(price + 0.01)


def test_breakeven_across_the_200_dollar_exemption_jump(cfg):
    item = ram(100)
    # 186.92 + 13.08 sales tax = exactly 200.00, still exempt (227.28 landed); one
    # cent more pays IVA on everything (~271), above the 250 target.
    assert exemption_price_limit(item, GRIP, settings(), cfg) == 186.92
    assert breakeven_price(item, 250.0, settings(), cfg, FranchisePool()) == 186.92
    # Without the jump the boundary is wherever the total meets the target.
    assert breakeven_price(item, 225.0, settings(), cfg, FranchisePool()) == 184.78


def test_breakeven_is_none_when_even_free_costs_too_much(cfg):
    assert breakeven_price(ram(100), 10.0, settings(), cfg, FranchisePool()) is None


def test_all_in_route_adds_only_the_card_fee(cfg):
    q = quote_all_in("tiendamia", 250.0, settings(card_fee_rate=0.03), cfg)
    assert q.total == 259.15  # 250 + 3% + IVA (9.15)
