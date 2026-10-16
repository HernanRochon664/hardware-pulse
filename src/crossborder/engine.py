"""
Landed cost of one shipment from a US store to Uruguay, line by line.

A *shipment* is what customs sees: one courier package (one or more orders,
consolidated) released at once. Its value V is the total of the store invoices:
price, the store's shipping to the warehouse, US sales tax and any
assisted-purchase fee (Decreto 50/026 art. 5). The courier's freight is not
part of V.

Regimes (configs/import_rules.yaml):
- franchise: 3 shipments and USD 800 per person and year, exempt from tariffs,
  IVA 22% on V with a USD 20 minimum. Exempt from every tax when all the items
  come from US sellers in the DNA registry and V <= USD 200 (TIFA).
- single tax ("prestación única"): 60% of V, minimum USD 20, any number of shipments.
Both need V <= USD 800 and <= 20 kg. Bigger shipments are formal imports with a
customs broker and are never priced here: ``Ineligible`` explains why instead.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from enum import StrEnum

from src.crossborder.config import (
    AllInRoute,
    Courier,
    CrossBorderConfig,
    RegistryStatus,
    RuleSet,
)
from src.crossborder.couriers import DeliveryOption, courier_charges
from src.crossborder.money import CostLine, cents, kg, pct, total, usd

_EPS = 1e-9
# Days for a US store to deliver to the courier's warehouse (added to its transit).
US_DOMESTIC_DAYS = (1, 5)


class Regime(StrEnum):
    FRANCHISE = "franchise"
    SINGLE_TAX = "single_tax"


REGIME_LABELS = {
    Regime.FRANCHISE: "Franquicia",
    Regime.SINGLE_TAX: "Prestación única (60%)",
}


@dataclass(frozen=True)
class ForeignItem:
    """A product bought in a US store. Price, weight and box are per unit."""

    label: str
    unit_price_usd: float
    store_id: str = "amazon"
    qty: int = 1
    store_shipping_usd: float = 0.0
    unit_weight_kg: float = 0.5
    unit_dims_cm: tuple[float, float, float] | None = None
    weight_estimated: bool = False

    def __post_init__(self) -> None:
        if self.unit_price_usd < 0 or self.store_shipping_usd < 0:
            raise ValueError(f"{self.label}: prices must not be negative")
        if self.qty < 1 or self.unit_weight_kg <= 0:
            raise ValueError(f"{self.label}: quantity and weight must be positive")

    @property
    def subtotal(self) -> float:
        return self.unit_price_usd * self.qty

    @property
    def weight_kg(self) -> float:
        return self.unit_weight_kg * self.qty

    @property
    def box_cm3(self) -> float | None:
        if not self.unit_dims_cm:
            return None
        length, width, height = self.unit_dims_cm
        return length * width * height * self.qty


@dataclass(frozen=True)
class Settings:
    """How the user buys: arrival date, courier, card, insurance and delivery."""

    arrival: date
    courier_id: str | None = None  # None: compare every courier and keep the cheapest
    sales_tax_exempt: bool = False  # the courier's DR-14 FAA certificate is registered
    card_fee_rate: float = 0.0
    card_fee_iva: bool = True
    insurance: bool = False
    delivery: DeliveryOption = "pickup"
    assisted_fee_rate: float = 0.0


@dataclass(frozen=True)
class FranchisePool:
    """What is left of one person's franchise in the year of the shipment."""

    holder: str = "Vos"
    shipments_left: int = 3
    usd_left: float = 800.0


@dataclass(frozen=True)
class ShipmentQuote:
    regime: Regime
    courier: Courier
    items: tuple[ForeignItem, ...]
    lines: tuple[CostLine, ...]
    invoice_usd: float
    real_kg: float
    chargeable_kg: float
    exempt: bool
    exemption_blockers: tuple[str, ...]
    notes: tuple[str, ...]
    rules: RuleSet

    @property
    def total(self) -> float:
        return total(self.lines)

    def amount(self, key: str) -> float:
        return cents(sum(line.amount for line in self.lines if line.key == key))

    @property
    def uy_tax(self) -> float:
        return self.amount("uy_tax")


@dataclass(frozen=True)
class Ineligible:
    regime: Regime
    courier_id: str
    reasons: tuple[str, ...]


Quote = ShipmentQuote | Ineligible


# ---------------------------------------------------------------------------
# One shipment
# ---------------------------------------------------------------------------


def store_lines(
    items: Sequence[ForeignItem], settings: Settings, courier: Courier, cfg: CrossBorderConfig
) -> tuple[list[CostLine], float, list[str]]:
    """Invoice lines (price, store shipping, sales tax, assisted fee), V and notes."""
    notes: list[str] = []
    lines = [
        CostLine(
            "price",
            "Precio en la tienda",
            cents(sum(item.subtotal for item in items)),
            _items_detail(items),
            None,
            "store",
        )
    ]
    shipping = sum(item.store_shipping_usd for item in items)
    if shipping:
        lines.append(CostLine("store_shipping", "Envío de la tienda al depósito", cents(shipping)))

    taxable = 0.0
    exempted = False
    for item in items:
        store = cfg.stores.stores.get(item.store_id)
        honors = store is not None and store.sales_tax_exemption_program
        if settings.sales_tax_exempt and courier.tax_free_certificate and honors:
            exempted = True
        else:
            taxable += item.subtotal + item.store_shipping_usd
    if courier.sales_tax_rate and taxable:
        lines.append(
            CostLine(
                "sales_tax",
                f"Sales tax {pct(courier.sales_tax_rate)}",
                cents(courier.sales_tax_rate * taxable),
                f"{pct(courier.sales_tax_rate)} de {usd(taxable)}: el depósito de "
                f"{courier.name} está en {courier.warehouse}",
                None,
                "store",
            )
        )
    if exempted:
        notes.append(
            f"Sin sales tax: certificado de exención de {courier.name} (DR-14 FAA) registrado "
            "en la tienda."
        )
    elif settings.sales_tax_exempt and not courier.tax_free_certificate:
        notes.append(f"{courier.name} no ofrece certificado de exención: se paga sales tax.")

    assisted = sum(item.subtotal for item in items if item.store_id == "assisted")
    if assisted and settings.assisted_fee_rate:
        lines.append(
            CostLine(
                "assisted_fee",
                "Comisión de compra asistida",
                cents(assisted * settings.assisted_fee_rate),
                f"{pct(settings.assisted_fee_rate)} de {usd(assisted)}",
            )
        )
    return lines, total(lines), notes


def exemption_blockers(
    items: Sequence[ForeignItem], invoice_usd: float, rules: RuleSet, cfg: CrossBorderConfig
) -> list[str]:
    """Why a franchise shipment does not get the US (TIFA) IVA exemption; [] if it does."""
    blockers: list[str] = []
    for store_id in sorted({item.store_id for item in items}):
        store = cfg.stores.stores.get(store_id)
        name = store.name if store else store_id
        if store is None or store.registry_status is RegistryStatus.NOT_LISTED:
            blockers.append(f"{name} no figura en el registro de vendedores de EE. UU. de Aduanas")
        elif store.registry_status is RegistryStatus.UNCERTAIN:
            blockers.append(f"{name}: la factura no la emite una empresa del registro de Aduanas")
    if invoice_usd > rules.treaty.max_invoice_usd + _EPS:
        blockers.append(
            f"el envío vale {usd(invoice_usd)}, más que el límite de "
            f"{usd(rules.treaty.max_invoice_usd)} por envío"
        )
    return blockers


def quote_shipment(
    items: Iterable[ForeignItem],
    regime: Regime,
    settings: Settings,
    courier: Courier,
    cfg: CrossBorderConfig,
    *,
    pool: FranchisePool | None = None,
) -> Quote:
    """
    Landed cost of one shipment under one regime and courier.

    ``pool`` is the franchise of the person receiving it; None skips the yearly
    franchise checks (the optimizer applies them across shipments).
    """
    shipment = tuple(items)
    if not shipment:
        raise ValueError("a shipment needs at least one item")
    rules = cfg.rules.for_date(settings.arrival)
    lines, invoice, notes = store_lines(shipment, settings, courier, cfg)
    real_kg = sum(item.weight_kg for item in shipment)

    reasons: list[str] = []
    limits = rules.franchise if regime is Regime.FRANCHISE else rules.single_tax
    if invoice > limits.max_invoice_usd + _EPS:
        reasons.append(
            f"el envío vale {usd(invoice)}: pasa los {usd(limits.max_invoice_usd)} por envío "
            "(sería una importación formal con despachante)"
        )
    if real_kg > limits.max_weight_kg + _EPS:
        reasons.append(f"pesa {kg(real_kg)}: pasa los {kg(limits.max_weight_kg)} por envío")
    if regime is Regime.FRANCHISE and pool is not None:
        if pool.shipments_left < 1:
            reasons.append(f"{pool.holder} no tiene envíos de franquicia disponibles este año")
        elif invoice > pool.usd_left + _EPS:
            reasons.append(
                f"a {pool.holder} le quedan {usd(pool.usd_left)} de franquicia y el envío vale "
                f"{usd(invoice)}"
            )

    box = sum(item.box_cm3 or 0.0 for item in shipment) or None
    iva_rate = rules.franchise.iva_rate
    charges = courier_charges(
        courier,
        real_kg=real_kg,
        box_cm3=box,
        invoice_usd=invoice,
        iva_rate=iva_rate,
        insurance=settings.insurance,
        delivery=settings.delivery,
    )
    if charges is None:
        reasons.append(f"{courier.name} no publica tarifa para {kg(real_kg)}")
    if reasons:
        return Ineligible(regime, courier.id, tuple(reasons))
    assert charges is not None

    if settings.card_fee_rate:
        fee = settings.card_fee_rate * invoice * (1 + iva_rate if settings.card_fee_iva else 1)
        detail = f"{pct(settings.card_fee_rate)} de {usd(invoice)}"
        if settings.card_fee_iva:
            detail += f" + IVA {pct(iva_rate)}"
        lines.append(
            CostLine(
                "card_fee",
                "Comisión de la tarjeta (compra en el exterior)",
                cents(fee),
                detail,
                None,
                "card",
            )
        )
    lines.extend(charges.lines)

    blockers: list[str] = []
    exempt = False
    if regime is Regime.FRANCHISE:
        blockers = exemption_blockers(shipment, invoice, rules, cfg)
        exempt = not blockers
        lines.append(_franchise_tax_line(invoice, exempt, rules, cfg))
    else:
        lines.append(_single_tax_line(invoice, rules))

    return ShipmentQuote(
        regime=regime,
        courier=courier,
        items=shipment,
        lines=tuple(lines),
        invoice_usd=invoice,
        real_kg=real_kg,
        chargeable_kg=charges.chargeable_kg,
        exempt=exempt,
        exemption_blockers=tuple(blockers),
        notes=tuple(notes) + charges.notes,
        rules=rules,
    )


def _franchise_tax_line(
    invoice: float, exempt: bool, rules: RuleSet, cfg: CrossBorderConfig
) -> CostLine:
    if exempt:
        return CostLine(
            "uy_tax",
            "IVA: exonerado (acuerdo con EE. UU.)",
            0.0,
            f"vendedor de EE. UU. registrado en Aduanas y envío de hasta "
            f"{usd(rules.treaty.max_invoice_usd)}",
            cfg.stores.registry.source_url,
            "tax",
        )
    franchise = rules.franchise
    iva = max(franchise.iva_rate * invoice, franchise.iva_min_usd)
    detail = f"{pct(franchise.iva_rate)} de {usd(invoice)}"
    if franchise.iva_rate * invoice < franchise.iva_min_usd:
        detail += f" = {usd(franchise.iva_rate * invoice)}; se paga el mínimo de " + usd(
            franchise.iva_min_usd
        )
    return CostLine(
        "uy_tax",
        "IVA de importación (franquicia)",
        cents(iva),
        detail,
        rules.source_url("iva_postal"),
        "tax",
    )


def _single_tax_line(invoice: float, rules: RuleSet) -> CostLine:
    single = rules.single_tax
    tax = max(single.rate * invoice, single.min_usd)
    detail = f"{pct(single.rate)} de {usd(invoice)}"
    if single.rate * invoice < single.min_usd:
        detail += f" = {usd(single.rate * invoice)}; se paga el mínimo de {usd(single.min_usd)}"
    return CostLine(
        "uy_tax", "Prestación única (60%)", cents(tax), detail, rules.source_url("decree"), "tax"
    )


def _items_detail(items: Sequence[ForeignItem]) -> str:
    parts = []
    for item in items:
        text = f"{item.label}: {usd(item.unit_price_usd)}"
        if item.qty > 1:
            text = f"{item.label}: {item.qty} × {usd(item.unit_price_usd)}"
        parts.append(text)
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Comparing couriers and regimes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Comparison:
    """Every courier × regime for the same items, cheapest first."""

    quotes: tuple[ShipmentQuote, ...]
    ineligible: tuple[Ineligible, ...]

    @property
    def best(self) -> ShipmentQuote | None:
        return self.quotes[0] if self.quotes else None

    @property
    def best_single_tax(self) -> ShipmentQuote | None:
        return next((q for q in self.quotes if q.regime is Regime.SINGLE_TAX), None)

    @property
    def best_franchise(self) -> ShipmentQuote | None:
        return next((q for q in self.quotes if q.regime is Regime.FRANCHISE), None)

    @property
    def franchise_value(self) -> float | None:
        """How much using a franchise shipment saves against the 60% regime."""
        franchise, single = self.best_franchise, self.best_single_tax
        if franchise is None or single is None:
            return None
        return cents(single.total - franchise.total)

    def best_by_courier(self) -> list[ShipmentQuote]:
        seen: dict[str, ShipmentQuote] = {}
        for quote in self.quotes:
            seen.setdefault(quote.courier.id, quote)
        return list(seen.values())


def couriers_for(settings: Settings, cfg: CrossBorderConfig) -> list[Courier]:
    if settings.courier_id:
        return [cfg.couriers[settings.courier_id]]
    return list(cfg.couriers.values())


def compare(
    items: Sequence[ForeignItem],
    settings: Settings,
    cfg: CrossBorderConfig,
    pool: FranchisePool | None,
) -> Comparison:
    """Quote every courier under the franchise (when ``pool`` is given) and the 60% regime."""
    regimes = [Regime.FRANCHISE, Regime.SINGLE_TAX] if pool is not None else [Regime.SINGLE_TAX]
    quotes: list[ShipmentQuote] = []
    ineligible: list[Ineligible] = []
    for courier in couriers_for(settings, cfg):
        for regime in regimes:
            result = quote_shipment(items, regime, settings, courier, cfg, pool=pool)
            if isinstance(result, ShipmentQuote):
                quotes.append(result)
            else:
                ineligible.append(result)
    # Ties go to the 60% regime, which keeps the franchise for later.
    quotes.sort(key=lambda q: (q.total, q.regime is Regime.FRANCHISE, q.courier.name))
    return Comparison(tuple(quotes), tuple(ineligible))


def breakeven_price(
    item: ForeignItem,
    target_total: float,
    settings: Settings,
    cfg: CrossBorderConfig,
    pool: FranchisePool | None,
) -> float | None:
    """
    Highest unit price (to the cent) at which importing ``item`` costs at most
    ``target_total``; None when not even a free product would.

    The cheapest landed cost never decreases when the price goes up (taxes,
    fees and regime changes only add), so a bisection finds the boundary, also
    across the jump at the USD 200 exemption limit.
    """

    def cost(price: float) -> float:
        best = compare([replace(item, unit_price_usd=price)], settings, cfg, pool).best
        return best.total if best else math.inf

    if cost(0.0) > target_total + _EPS:
        return None
    # Above this price the invoice passes USD 800 and nothing is eligible (cost = inf).
    lo, hi = 0.0, cfg.rules.for_date(settings.arrival).single_tax.max_invoice_usd / item.qty + 1
    for _ in range(50):
        mid = (lo + hi) / 2
        if cost(mid) <= target_total + _EPS:
            lo = mid
        else:
            hi = mid
    return math.floor(lo * 100 + 1e-6) / 100


def exemption_price_limit(
    item: ForeignItem, courier: Courier, settings: Settings, cfg: CrossBorderConfig
) -> float:
    """
    Highest unit price (to the cent) that keeps the invoice within the USD 200
    exemption limit, with the sales tax rounded to cents as on the invoice.
    """
    limit = cfg.rules.for_date(settings.arrival).treaty.max_invoice_usd

    def invoice(price: float) -> float:
        return store_lines([replace(item, unit_price_usd=price)], settings, courier, cfg)[1]

    store = cfg.stores.stores.get(item.store_id)
    honors = store is not None and store.sales_tax_exemption_program
    exempt = settings.sales_tax_exempt and courier.tax_free_certificate and honors
    rate = 0.0 if exempt else courier.sales_tax_rate
    price = max(math.floor((limit / (1 + rate) - item.store_shipping_usd) / item.qty * 100), 0)
    while price > 0 and invoice(price / 100) > limit + _EPS:
        price -= 1
    while invoice((price + 1) / 100) <= limit + _EPS:
        price += 1
    return price / 100


# ---------------------------------------------------------------------------
# All-in routes and dates
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AllInQuote:
    route: AllInRoute
    lines: tuple[CostLine, ...]

    @property
    def total(self) -> float:
        return total(self.lines)


def quote_all_in(
    route_id: str, final_usd: float, settings: Settings, cfg: CrossBorderConfig
) -> AllInQuote:
    """A price that already includes freight and taxes (Tiendamia, Amazon direct)."""
    route = cfg.stores.all_in[route_id]
    lines = [
        CostLine("all_in", f"Precio final: {route.name}", cents(final_usd), route.note, route.site)
    ]
    if settings.card_fee_rate:
        iva_rate = cfg.rules.for_date(settings.arrival).franchise.iva_rate
        fee = settings.card_fee_rate * final_usd * (1 + iva_rate if settings.card_fee_iva else 1)
        detail = f"{pct(settings.card_fee_rate)} de {usd(final_usd)}"
        if settings.card_fee_iva:
            detail += f" + IVA {pct(iva_rate)}"
        lines.append(
            CostLine(
                "card_fee",
                "Comisión de la tarjeta (compra en el exterior)",
                cents(fee),
                detail,
                None,
                "card",
            )
        )
    return AllInQuote(route, tuple(lines))


def estimated_arrival(today: date, settings: Settings, cfg: CrossBorderConfig) -> date:
    """Latest likely release date: US delivery to the warehouse plus the slowest transit."""
    slowest = max(courier.transit_days[1] for courier in couriers_for(settings, cfg))
    return today + timedelta(days=US_DOMESTIC_DAYS[1] + slowest)
