"""
"¿Conviene importar?": the verdict, alerts about hidden costs and the risks.

The verdict compares money only: the landed cost against the best local price,
and it asks for a minimum saving (the user's comfort threshold) before saying
"conviene importar". Warranty, returns, delays and loss are listed apart as
risks and never added to the total.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from enum import StrEnum
from typing import Literal

from src.crossborder.config import CrossBorderConfig, RegistryStatus
from src.crossborder.engine import (
    US_DOMESTIC_DAYS,
    Comparison,
    FranchisePool,
    Regime,
    Settings,
    ShipmentQuote,
    compare,
    exemption_price_limit,
)
from src.crossborder.money import pct, usd

STALE_AFTER_DAYS = 60
# A box this many times bigger than its weight may be charged by volume.
VOLUME_WARNING_RATIO = 1.5
VOLUME_WARNING_DIVISOR = 5000


@dataclass(frozen=True)
class Threshold:
    """Minimum saving that makes importing worth it (the larger of both)."""

    min_usd: float = 25.0
    min_pct: float = 0.10

    def required(self, local_usd: float) -> float:
        return max(self.min_usd, self.min_pct * local_usd)


class VerdictCode(StrEnum):
    IMPORT = "import"
    SMALL_GAP = "small_gap"
    BUY_LOCAL = "buy_local"
    ONLY_ABROAD = "only_abroad"
    NOT_BY_COURIER = "not_by_courier"
    NO_PRICES = "no_prices"


@dataclass(frozen=True)
class Verdict:
    code: VerdictCode
    title: str
    detail: str
    saving_usd: float | None = None
    saving_pct: float | None = None


OVER_LIMIT_ADVICE = (
    "Si son varios artículos, dividilos en envíos de hasta U$S 800 (pestaña «Armado»).",
    "Cada persona tiene su propia franquicia (3 envíos y U$S 800 por año): un familiar mayor "
    "de edad puede comprar con su tarjeta internacional y recibirlo a su nombre.",
    "Las vías con precio final (Tiendamia, Amazon con envío directo) muestran el total antes "
    "de pagar.",
    "Nunca dividas un mismo artículo ni declares menos valor: la multa es el doble de los "
    "tributos y se pierde la franquicia.",
)


def judge(
    local_usd: float | None,
    import_usd: float | None,
    threshold: Threshold,
    ineligible_reasons: Sequence[str] = (),
) -> Verdict:
    if import_usd is None:
        if ineligible_reasons:
            return Verdict(
                VerdictCode.NOT_BY_COURIER,
                "No entra por courier",
                _sentence("; ".join(dict.fromkeys(ineligible_reasons))) + ".",
            )
        return Verdict(VerdictCode.NO_PRICES, "Faltan datos", "Ingresá el precio en EE. UU.")
    if local_usd is None:
        return Verdict(
            VerdictCode.ONLY_ABROAD,
            "Hoy no se consigue en Uruguay",
            f"Importado te cuesta {usd(import_usd)} puesto acá.",
        )
    saving = round(local_usd - import_usd, 2)
    share = saving / local_usd if local_usd else 0.0
    need = threshold.required(local_usd)
    if saving >= need:
        return Verdict(
            VerdictCode.IMPORT,
            "Conviene importar",
            f"Ahorrás {usd(saving)} ({pct(share)}) frente al mejor precio en Uruguay; tu "
            f"umbral es {usd(need)}.",
            saving,
            share,
        )
    if saving > 0:
        return Verdict(
            VerdictCode.SMALL_GAP,
            "Diferencia chica: conviene comprar acá",
            f"Importar ahorra {usd(saving)} ({pct(share)}), menos que tu umbral de "
            f"{usd(need)}: por garantía, plazo y devoluciones conviene comprar acá.",
            saving,
            share,
        )
    return Verdict(
        VerdictCode.BUY_LOCAL,
        "Conviene comprar acá",
        f"Importado cuesta {usd(-saving)} más ({pct(-share)}) que el mejor precio en Uruguay.",
        saving,
        share,
    )


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Alert:
    level: Literal["info", "warning"]
    text: str


def package_alerts(quote: ShipmentQuote) -> list[Alert]:
    """Estimated weights and bulky boxes (monitors, cases): shared with the cart plan."""
    out: list[Alert] = []
    if any(i.weight_estimated for i in quote.items):
        out.append(
            Alert(
                "warning",
                "Peso y medidas estimados para la categoría: el flete depende del peso. Si lo "
                "sabés (figura en la ficha de la tienda), corregilo.",
            )
        )

    box = sum(i.box_cm3 or 0.0 for i in quote.items)
    if (
        quote.courier.volumetric is None
        and box
        and box / VOLUME_WARNING_DIVISOR > VOLUME_WARNING_RATIO * quote.real_kg
    ):
        out.append(
            Alert(
                "info",
                f"La caja es grande para su peso: {quote.courier.name} podría cobrar peso "
                "volumétrico (no publica la regla).",
            )
        )
    return out


def alerts(
    quote: ShipmentQuote,
    comparison: Comparison,
    settings: Settings,
    cfg: CrossBorderConfig,
    *,
    today: date,
    pool: FranchisePool | None,
) -> list[Alert]:
    """Hidden costs and assumptions behind one quote, most important first."""
    out: list[Alert] = []
    rules = quote.rules
    item = quote.items[0] if len(quote.items) == 1 else None

    if quote.regime is Regime.FRANCHISE and not quote.exempt:
        listed = all(
            (store := cfg.stores.stores.get(i.store_id)) is not None
            and store.registry_status is RegistryStatus.LISTED
            for i in quote.items
        )
        if listed and quote.invoice_usd > rules.treaty.max_invoice_usd:
            over = quote.invoice_usd - rules.treaty.max_invoice_usd
            text = (
                f"El envío vale {usd(quote.invoice_usd)}: te pasás por {usd(over)} del límite "
                f"de {usd(rules.treaty.max_invoice_usd)} para no pagar IVA (el sales tax y el "
                "envío de la tienda cuentan)."
            )
            if item is not None:
                limit = exemption_price_limit(item, quote.courier, settings, cfg)
                if limit > 0:
                    text += f" Con un precio de hasta {usd(limit)} entraría exento."
                tax_free = [c for c in cfg.couriers.values() if c.tax_free_certificate]
                store = cfg.stores.stores.get(item.store_id)
                if (
                    not settings.sales_tax_exempt
                    and tax_free
                    and store is not None
                    and store.sales_tax_exemption_program
                    and item.subtotal + item.store_shipping_usd <= rules.treaty.max_invoice_usd
                ):
                    names = ", ".join(c.name for c in tax_free)
                    text += (
                        f" O sin sales tax: {names} da un certificado de exención que se "
                        "registra en la tienda."
                    )
            out.append(Alert("warning", text))
        elif not listed:
            saving = _saving_if_registered(quote, settings, cfg, pool)
            text = _sentence("; ".join(quote.exemption_blockers)) + ": se calcula con IVA."
            if saving:
                text += (
                    f" Comprado a un vendedor registrado (p. ej. «Vendido por Amazon.com» o la "
                    f"compra asistida de un courier) ahorrarías {usd(saving)}."
                )
            if settings.arrival < rules.treaty.registry_required_from:
                deadline = rules.treaty.registry_required_from - timedelta(days=1)
                text += (
                    f" Hasta el {deadline:%d/%m/%Y} alcanza con que el vendedor tenga residencia "
                    "fiscal en EE. UU.; después tiene que figurar en el registro de Aduanas."
                )
            out.append(Alert("warning", text))

    out += package_alerts(quote)

    if quote.regime is Regime.FRANCHISE and pool is not None:
        left_usd = pool.usd_left - quote.invoice_usd
        left_n = pool.shipments_left - 1
        text = (
            f"Usa 1 de los envíos de franquicia de {pool.holder}: quedarían {left_n} "
            f"{'envío' if left_n == 1 else 'envíos'} y {usd(left_usd)} para el resto del año."
        )
        value = comparison.franchise_value
        if value and value > 0:
            text += f" Sin franquicia (régimen del 60%) pagarías {usd(value)} más."
        out.append(Alert("info", text))

    if settings.arrival.year > today.year:
        out.append(
            Alert(
                "info",
                f"Llegaría en {settings.arrival.year}: usa el cupo de franquicia de ese año, que "
                "empieza de cero el 1 de enero.",
            )
        )

    for note in quote.notes:
        out.append(Alert("info", note))

    if (today - quote.courier.verified_at).days > STALE_AFTER_DAYS:
        out.append(
            Alert(
                "info",
                f"La tarifa de {quote.courier.name} se verificó el "
                f"{quote.courier.verified_at:%d/%m/%Y}: puede haber cambiado.",
            )
        )
    if (today - rules.verified_at).days > STALE_AFTER_DAYS:
        out.append(
            Alert(
                "info",
                f"Las reglas de Aduanas se verificaron el {rules.verified_at:%d/%m/%Y}.",
            )
        )
    return out


def _sentence(text: str) -> str:
    """Uppercase the first letter only (str.capitalize would lowercase 'U$S')."""
    return text[:1].upper() + text[1:]


def _saving_if_registered(
    quote: ShipmentQuote, settings: Settings, cfg: CrossBorderConfig, pool: FranchisePool | None
) -> float | None:
    """How much cheaper the same shipment would be from sellers in the registry."""
    items = [replace(i, store_id="amazon") for i in quote.items]
    best = compare(items, settings, cfg, pool).best
    if best is None:
        return None
    saving = round(quote.total - best.total, 2)
    return saving if saving > 0 else None


# ---------------------------------------------------------------------------
# Risks (shown apart from the total)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Risk:
    icon: str
    title: str
    text: str


def risks(quote: ShipmentQuote | None) -> list[Risk]:
    if quote is None:
        days = "una a tres semanas"
    else:
        low = US_DOMESTIC_DAYS[0] + quote.courier.transit_days[0]
        high = US_DOMESTIC_DAYS[1] + quote.courier.transit_days[1]
        days = f"entre {low} y {high} días (envío de la tienda al depósito + {quote.courier.name})"
    return [
        Risk(
            ":material/schedule:",
            "Plazo",
            f"Llega en {days}. Comprado acá te lo llevás hoy o mañana.",
        ),
        Risk(
            ":material/verified_user:",
            "Garantía",
            "La da el fabricante en EE. UU.: si falla, el envío de ida y vuelta corre por tu "
            "cuenta. Comprado acá, reclamás en la tienda.",
        ),
        Risk(
            ":material/undo:",
            "Devoluciones",
            "Devolver desde Uruguay cuesta un flete internacional y los impuestos pagados no se "
            "recuperan. Mientras siga en el depósito de Miami, el courier puede devolverlo por "
            "un cargo.",
        ),
        Risk(
            ":material/inventory_2:",
            "Pérdida o daño",
            "Es poco común, pero pasa. Algunos couriers ofrecen seguro (Aerobox: 3% del valor, "
            "mínimo U$S 33).",
        ),
        Risk(
            ":material/badge:",
            "Requisitos",
            "Pagás con tarjeta internacional a tu nombre, el paquete viene a tu nombre y tenés "
            "que estar registrado en Aduanas (identidad digital).",
        ),
    ]
