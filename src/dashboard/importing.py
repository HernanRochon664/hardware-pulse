"""
Import calculator UI ("¿comprar acá o importar?"), shared by the Importar page
and the dialog of the Producto page. Every number comes from src/crossborder:
this module only collects the inputs and shows the results.

The user's situation (franchise already used this year, courier, card...) is
kept in st.session_state under "imp_*" keys so it survives page changes;
widgets use separate "w_imp_*" keys because Streamlit drops widget state when a
page stops rendering them.
"""

from __future__ import annotations

import functools
import html as html_lib
import re
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import streamlit as st
import streamlit.components.v1 as st_components

from src.crossborder.config import CrossBorderConfig, load_config
from src.crossborder.couriers import DeliveryOption
from src.crossborder.engine import (
    REGIME_LABELS,
    US_DOMESTIC_DAYS,
    Comparison,
    ForeignItem,
    FranchisePool,
    Settings,
    ShipmentQuote,
    breakeven_price,
    compare,
    estimated_arrival,
    quote_all_in,
)
from src.crossborder.intake import CatalogMatch, CatalogProduct, match_catalog, parse_intake
from src.crossborder.money import GROUP_LABELS, CostLine
from src.crossborder.optimizer import MAX_UNITS, CartItem, CartResult, optimize
from src.crossborder.verdict import (
    OVER_LIMIT_ADVICE,
    Threshold,
    Verdict,
    VerdictCode,
    alerts,
    judge,
    risks,
)
from src.dashboard.components import cost_waterfall
from src.dashboard.formatting import md, money, pct
from src.settings import LOCAL_TZ

APP_URL = "https://hardware-pulse.streamlit.app"
BOOKMARKLET_PATH = Path(__file__).parent / "static" / "bookmarklet.js"

STORE_CHOICES = (
    "amazon",
    "amazon_marketplace",
    "newegg",
    "bhphoto",
    "bestbuy",
    "microcenter",
    "other",
)
SEARCH_STORES = ("amazon", "newegg", "bhphoto", "bestbuy", "microcenter")
ROUTES = {
    "courier": "Casilla de courier",
    "assisted": "Compra asistida",
    "tiendamia": "Tiendamia",
    "amazon_direct": "Amazon directo",
}
CARD_FEES = {0.0: "Sin comisión", 0.015: "1,5% + IVA", 0.03: "3% + IVA"}
DELIVERY = {
    "pickup": "Retiro en el local del courier",
    "montevideo": "A domicilio en Montevideo",
    "interior": "Interior",
}
PEOPLE_COLUMNS = ("Persona", "Envíos usados", "U$S usados")
DEFAULT_PEOPLE = [{"Persona": "Vos", "Envíos usados": 0, "U$S usados": 0.0}]
VERDICT_ICONS = {
    VerdictCode.IMPORT: ":material/flight_land:",
    VerdictCode.SMALL_GAP: ":material/balance:",
    VerdictCode.BUY_LOCAL: ":material/storefront:",
    VerdictCode.ONLY_ABROAD: ":material/public:",
    VerdictCode.NOT_BY_COURIER: ":material/block:",
    VerdictCode.NO_PRICES: ":material/edit:",
}
_MD_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+\-.!|<>~$])")


def escape_md(text: str) -> str:
    """Text from outside (a store page title) shown literally in markdown."""
    return _MD_SPECIAL.sub(r"\\\1", text)


def today_local() -> date:
    return datetime.now(LOCAL_TZ).date()


# ---------------------------------------------------------------------------
# The user's situation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Person:
    name: str
    used_shipments: int = 0
    used_usd: float = 0.0


@dataclass(frozen=True)
class Prefs:
    people: tuple[Person, ...] = (Person("Vos"),)
    courier_id: str | None = None
    card_fee_rate: float = 0.0
    tax_free: bool = False
    insurance: bool = False
    delivery: str = "pickup"
    threshold: Threshold = Threshold()

    def settings(self, today: date, cfg: CrossBorderConfig) -> Settings:
        base = Settings(
            arrival=today,
            courier_id=self.courier_id,
            sales_tax_exempt=self.tax_free,
            card_fee_rate=self.card_fee_rate,
            insurance=self.insurance,
            delivery=cast(DeliveryOption, self.delivery),
        )
        return replace(base, arrival=estimated_arrival(today, base, cfg))

    def pools(self, cfg: CrossBorderConfig, arrival: date, today: date) -> list[FranchisePool]:
        """What is left of each person's franchise in the year the shipment arrives."""
        franchise = cfg.rules.for_date(arrival).franchise
        new_year = arrival.year > today.year
        return [
            FranchisePool(
                person.name,
                franchise.max_shipments
                if new_year
                else max(franchise.max_shipments - person.used_shipments, 0),
                franchise.annual_usd_cap
                if new_year
                else max(franchise.annual_usd_cap - person.used_usd, 0.0),
            )
            for person in self.people
        ]


def current_prefs() -> Prefs:
    state = st.session_state
    people = []
    for i, row in enumerate(state.get("imp_people", DEFAULT_PEOPLE)):
        name = str(row.get("Persona") or "").strip() or f"Persona {i + 1}"
        people.append(
            Person(name, int(row.get("Envíos usados") or 0), float(row.get("U$S usados") or 0.0))
        )
    return Prefs(
        people=tuple(people) or (Person("Vos"),),
        courier_id=state.get("imp_courier"),
        card_fee_rate=float(state.get("imp_card_fee", 0.0)),
        tax_free=bool(state.get("imp_tax_free", False)),
        insurance=bool(state.get("imp_insurance", False)),
        delivery=str(state.get("imp_delivery", "pickup")),
        threshold=Threshold(
            float(state.get("imp_min_usd", 25.0)), float(state.get("imp_min_pct", 10.0)) / 100
        ),
    )


def _remember(key: str, value: object) -> None:
    st.session_state[key] = value


def prefs_controls(*, compact: bool = False) -> Prefs:
    """Widgets for the user's situation; returns the resulting Prefs."""
    if compact:
        _franchise_controls(compact=True)
        _buying_controls(compact=True)
        _threshold_controls()
    else:
        cols = st.columns([5, 4, 3], gap="large")
        with cols[0]:
            _franchise_controls(compact=False)
        with cols[1]:
            _buying_controls(compact=False)
        with cols[2]:
            _threshold_controls()
    return current_prefs()


def _franchise_controls(*, compact: bool) -> None:
    state = st.session_state
    st.markdown("**Tu franquicia de este año**")
    st.caption(
        "Cada persona mayor de edad tiene 3 envíos y U$S 800 por año. Contá también lo que "
        "trajiste antes de mayo."
    )
    if compact:
        people = state.get("imp_people", DEFAULT_PEOPLE)
        first = dict(people[0])
        cols = st.columns(2)
        first["Envíos usados"] = cols[0].number_input(
            "Envíos ya usados",
            min_value=0,
            max_value=3,
            value=int(first.get("Envíos usados") or 0),
            key="w_imp_used_n",
        )
        first["U$S usados"] = cols[1].number_input(
            "U$S ya usados",
            min_value=0.0,
            max_value=800.0,
            value=float(first.get("U$S usados") or 0.0),
            step=10.0,
            key="w_imp_used_usd",
        )
        _remember("imp_people", [first, *people[1:]])
    else:
        if "w_imp_people" not in state:
            state["imp_people_base"] = pd.DataFrame(
                state.get("imp_people", DEFAULT_PEOPLE), columns=list(PEOPLE_COLUMNS)
            )
        edited = st.data_editor(
            state["imp_people_base"],
            key="w_imp_people",
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "Persona": st.column_config.TextColumn(required=True),
                "Envíos usados": st.column_config.NumberColumn(min_value=0, max_value=3, step=1),
                "U$S usados": st.column_config.NumberColumn(
                    min_value=0.0, max_value=800.0, step=10.0, format="%.2f"
                ),
            },
        )
        people = edited.head(4).fillna({"Envíos usados": 0, "U$S usados": 0.0})
        _remember("imp_people", people.to_dict("records"))
        st.caption(
            "Podés sumar familiares: cada uno compra con su tarjeta internacional y lo recibe a "
            "su nombre (máximo 4 personas)."
        )


def _buying_controls(*, compact: bool) -> None:
    cfg = load_config()
    state = st.session_state
    st.markdown("**Cómo comprás**")
    couriers = {"": "El más barato", **{cid: c.name for cid, c in cfg.couriers.items()}}
    current = state.get("imp_courier") or ""
    courier = st.selectbox(
        "Courier",
        list(couriers),
        index=list(couriers).index(current) if current in couriers else 0,
        format_func=couriers.__getitem__,
        key="w_imp_courier",
    )
    _remember("imp_courier", courier or None)
    fees = list(CARD_FEES)
    fee = st.selectbox(
        "Comisión de tu tarjeta por compras en el exterior",
        fees,
        index=fees.index(state.get("imp_card_fee", 0.0)),
        format_func=CARD_FEES.__getitem__,
        key="w_imp_card_fee",
        help="Algunas tarjetas cobran 1,5% o 3% + IVA (ver la cartilla de tu banco).",
    )
    _remember("imp_card_fee", fee)
    tax_free = st.toggle(
        "Tengo registrada la exención de sales tax del courier",
        value=bool(state.get("imp_tax_free", False)),
        key="w_imp_tax_free",
        help=(
            "Grinbox da un certificado de Florida (DR-14 FAA) que se registra en Amazon, eBay, "
            "Best Buy, Newegg y B&H: esas compras no pagan el 7% de sales tax."
        ),
    )
    _remember("imp_tax_free", tax_free)
    if not compact:
        insurance = st.toggle(
            "Contratar seguro",
            value=bool(state.get("imp_insurance", False)),
            key="w_imp_insurance",
            help="Solo se suma en los couriers que publican su costo.",
        )
        _remember("imp_insurance", insurance)
        options = list(DELIVERY)
        delivery = st.selectbox(
            "Entrega",
            options,
            index=options.index(state.get("imp_delivery", "pickup")),
            format_func=DELIVERY.__getitem__,
            key="w_imp_delivery",
        )
        _remember("imp_delivery", delivery)


def _threshold_controls() -> None:
    state = st.session_state
    st.markdown("**Cuánto tiene que ahorrar para que valga la pena**")
    cols = st.columns(2)
    min_usd = cols[0].number_input(
        "Mínimo U$S",
        min_value=0.0,
        value=float(state.get("imp_min_usd", 25.0)),
        step=5.0,
        key="w_imp_min_usd",
    )
    min_pct = cols[1].number_input(
        "Mínimo %",
        min_value=0.0,
        max_value=100.0,
        value=float(state.get("imp_min_pct", 10.0)),
        step=1.0,
        key="w_imp_min_pct",
        help="Se usa el mayor de los dos: compensa garantía, plazo y devoluciones.",
    )
    _remember("imp_min_usd", min_usd)
    _remember("imp_min_pct", min_pct)


# ---------------------------------------------------------------------------
# One product
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Prefill:
    key: str
    label: str
    category: str | None = None
    family_id: str | None = None
    mpn: str | None = None
    local_usd: float | None = None
    local_store: str | None = None
    us_price: float | None = None
    store_id: str = "amazon"
    weight_kg: float | None = None
    dims_cm: tuple[float, float, float] | None = None
    url: str | None = None


def search_links(label: str, mpn: str | None) -> None:
    cfg = load_config()
    query = mpn or label
    with st.container(horizontal=True):
        for store_id in SEARCH_STORES:
            store = cfg.stores.stores[store_id]
            link = store.search_link(query)
            if link:
                name = store.name.split(" (")[0]
                st.link_button(f"Buscar en {name}", link, icon=":material/search:")


@functools.lru_cache(maxsize=1024)
def import_targets(
    local_usd: float,
    family_id: str | None,
    category: str | None,
    prefs: Prefs,
    today: date,
) -> tuple[float | None, float | None]:
    """(break-even, worth-it) unit price in a registered US store, default package."""
    cfg = load_config()
    settings = prefs.settings(today, cfg)
    pools = prefs.pools(cfg, settings.arrival, today)
    profile = cfg.profiles.for_product(family_id, category)
    item = ForeignItem(
        "producto", 0.0, unit_weight_kg=profile.weight_kg, unit_dims_cm=profile.dims_cm
    )
    pool = pools[0] if pools else None
    even = breakeven_price(item, local_usd, settings, cfg, pool)
    worth = breakeven_price(
        item, local_usd - prefs.threshold.required(local_usd), settings, cfg, pool
    )
    return even, worth


def targets_text(local_usd: float, even: float | None, worth: float | None, rate) -> str:
    if worth is not None and worth > 0:
        text = (
            f"Importarlo conviene si en EE. UU. lo conseguís a **{money(worth, rate)}** o menos "
            "en una tienda registrada (p. ej. vendido por Amazon.com)"
        )
        if even is not None:
            text += f"; a {money(even, rate)} da lo mismo que comprarlo acá"
        return text + "."
    if even is not None and even > 0:
        return (
            f"Importarlo empata recién si en EE. UU. está a {money(even, rate)}: no llega a "
            "ahorrar lo suficiente para que valga la pena."
        )
    return "Ni regalado conviene importarlo: el envío y las tasas superan el precio de acá."


def single_calculator(prefill: Prefill, prefs: Prefs, *, scope: str, rate) -> None:
    cfg = load_config()
    today = today_local()
    key = f"{scope}_{prefill.key}"
    settings = prefs.settings(today, cfg)
    pools = prefs.pools(cfg, settings.arrival, today)
    route = (
        st.segmented_control(
            "¿Cómo lo comprás?",
            list(ROUTES),
            format_func=ROUTES.__getitem__,
            default="courier",
            key=f"{key}_route",
        )
        or "courier"
    )

    if route in cfg.stores.all_in:
        final = st.number_input(
            "Precio final puesto en Uruguay (U$S)",
            min_value=0.0,
            value=None,
            step=1.0,
            key=f"{key}_final",
        )
        st.caption(md(cfg.stores.all_in[route].note))
        if final:
            quote = quote_all_in(route, final, settings, cfg)
            local = prefill.local_usd
            show_verdict(judge(local, quote.total, prefs.threshold))
            show_lines(list(quote.lines), rate)
            st.info(
                md(
                    "Esta vía también usa una de tus franquicias del año (o paga el 60% si ya "
                    "no te quedan)."
                ),
                icon=":material/info:",
            )
        return

    profile = cfg.profiles.for_product(prefill.family_id, prefill.category)
    estimated_default = prefill.weight_kg is None
    weight_default = prefill.weight_kg or profile.weight_kg
    dims_default = prefill.dims_cm or profile.dims_cm

    cols = st.columns([2, 2, 1])
    fee = 0.0
    if route == "assisted":
        store = "assisted"
        fee = cols[0].number_input(
            "Comisión de la compra asistida (%)",
            min_value=0.0,
            max_value=50.0,
            value=0.0,
            step=0.5,
            key=f"{key}_fee",
            help=cfg.stores.stores["assisted"].note,
        )
    else:
        default = prefill.store_id if prefill.store_id in STORE_CHOICES else "amazon"
        store = cols[0].selectbox(
            "Tienda en EE. UU.",
            STORE_CHOICES,
            index=STORE_CHOICES.index(default),
            format_func=lambda s: cfg.stores.stores[s].name,
            key=f"{key}_store",
        )
    price = cols[1].number_input(
        "Precio en la tienda (U$S, sin impuestos)",
        min_value=0.0,
        value=prefill.us_price,
        step=1.0,
        format="%.2f",
        key=f"{key}_price",
    )
    qty = int(
        cols[2].number_input(
            "Cantidad", min_value=1, max_value=MAX_UNITS, value=1, key=f"{key}_qty"
        )
    )
    note = cfg.stores.stores[store].note
    if note:
        st.caption(md(note))

    title = "Peso, medidas y envío de la tienda" + (" (estimados)" if estimated_default else "")
    with st.expander(title):
        cols = st.columns(5)
        weight = cols[0].number_input(
            "Peso por unidad (kg)",
            min_value=0.01,
            value=float(weight_default),
            step=0.05,
            key=f"{key}_weight",
        )
        dims = tuple(
            cols[i + 1].number_input(
                label, min_value=0.0, value=float(dims_default[i]), step=1.0, key=f"{key}_dim{i}"
            )
            for i, label in enumerate(("Largo (cm)", "Ancho (cm)", "Alto (cm)"))
        )
        shipping = cols[4].number_input(
            "Envío de la tienda (U$S)",
            min_value=0.0,
            value=0.0,
            step=1.0,
            key=f"{key}_shipping",
            help="Lo que cobra la tienda por llevarlo al depósito del courier en Miami.",
        )
        if estimated_default:
            st.caption(
                md(
                    f"Estimado para «{profile.label}». Si la ficha de la tienda trae el peso y "
                    "las medidas del paquete, usalos: el flete depende de eso."
                )
            )
    use_franchise = st.toggle(
        "Usar mi franquicia si conviene",
        value=True,
        key=f"{key}_franchise",
        help="Desactivalo para guardarla para una compra más cara: se calcula con el 60%.",
    )
    search_links(prefill.label, prefill.mpn)

    local_total = prefill.local_usd * qty if prefill.local_usd else None
    if not price:
        if prefill.local_usd:
            even, worth = import_targets(
                prefill.local_usd, prefill.family_id, prefill.category, prefs, today
            )
            st.info(md(targets_text(prefill.local_usd, even, worth, rate)), icon=":material/sell:")
        else:
            st.info("Ingresá el precio en EE. UU. para calcular.", icon=":material/edit:")
        return

    unit_dims = (float(dims[0]), float(dims[1]), float(dims[2]))
    item = ForeignItem(
        prefill.label,
        float(price),
        store_id=store,
        qty=qty,
        store_shipping_usd=float(shipping),
        unit_weight_kg=float(weight),
        unit_dims_cm=unit_dims if all(unit_dims) else None,
        weight_estimated=estimated_default
        and float(weight) == float(weight_default)
        and unit_dims == tuple(float(d) for d in dims_default),
    )
    settings = replace(settings, assisted_fee_rate=fee / 100)
    pool = pools[0] if use_franchise and pools else None
    comparison = compare([item], settings, cfg, pool)
    best = comparison.best
    reasons = [reason for result in comparison.ineligible for reason in result.reasons]
    verdict = judge(local_total, best.total if best else None, prefs.threshold, reasons)
    show_verdict(verdict, _over_limit_advice(item, settings, cfg))
    if best is None:
        return
    show_quote(best, comparison, settings, cfg, today, pool, local_total, prefill.local_store, rate)
    if prefill.local_usd:
        even, worth = _targets_for(item, prefill.local_usd, settings, cfg, pool, prefs)
        st.caption(md(targets_text(prefill.local_usd, even, worth, rate)))
    show_risks(best)


def _over_limit_advice(
    item: ForeignItem, settings: Settings, cfg: CrossBorderConfig
) -> tuple[str, ...]:
    """Ways around the USD 800 limit, starting with the sales tax when it is what tips it."""
    limit = cfg.rules.for_date(settings.arrival).single_tax.max_invoice_usd
    store = cfg.stores.stores.get(item.store_id)
    untaxed = item.subtotal + item.store_shipping_usd
    tax_free = [c.name for c in cfg.couriers.values() if c.tax_free_certificate]
    advice = OVER_LIMIT_ADVICE
    if (
        untaxed <= limit
        and not settings.sales_tax_exempt
        and tax_free
        and store is not None
        and store.sales_tax_exemption_program
    ):
        advice = (
            f"Sin sales tax la factura sería de {money(untaxed, cur='USD')} y entraría por "
            f"courier: {', '.join(tax_free)} tiene un certificado de exención que se registra "
            "en la tienda.",
            *advice,
        )
    return advice


def _targets_for(
    item: ForeignItem,
    local_unit_usd: float,
    settings: Settings,
    cfg: CrossBorderConfig,
    pool: FranchisePool | None,
    prefs: Prefs,
) -> tuple[float | None, float | None]:
    local = local_unit_usd * item.qty
    even = breakeven_price(item, local, settings, cfg, pool)
    worth = breakeven_price(item, local - prefs.threshold.required(local), settings, cfg, pool)
    return even, worth


def show_verdict(verdict: Verdict, advice: tuple[str, ...] = ()) -> None:
    box: Callable = {
        VerdictCode.IMPORT: st.success,
        VerdictCode.SMALL_GAP: st.warning,
        VerdictCode.NOT_BY_COURIER: st.error,
    }.get(verdict.code, st.info)
    body = f"**{verdict.title}.** {verdict.detail}"
    if verdict.code is VerdictCode.NOT_BY_COURIER:
        body += "\n\n" + "\n".join(f"- {line}" for line in (advice or OVER_LIMIT_ADVICE))
    box(md(body), icon=VERDICT_ICONS[verdict.code])


def show_lines(lines: list[CostLine], rate) -> None:
    table = pd.DataFrame(
        {
            "Grupo": [GROUP_LABELS[line.group] for line in lines],
            "Concepto": [line.label for line in lines],
            "Monto": [money(line.amount, rate) for line in lines],
            "Cómo se calcula": [line.detail for line in lines],
            "Fuente": [line.source for line in lines],
        }
    )
    st.dataframe(
        table,
        hide_index=True,
        column_config={"Fuente": st.column_config.LinkColumn("Fuente", display_text="Ver ↗")},
    )


def show_quote(
    quote: ShipmentQuote,
    comparison: Comparison,
    settings: Settings,
    cfg: CrossBorderConfig,
    today: date,
    pool: FranchisePool | None,
    local_total: float | None,
    local_store: str | None,
    rate,
) -> None:
    cols = st.columns(3)
    cols[0].metric(
        "Importado, puesto en Uruguay",
        money(quote.total, rate),
        help=f"{REGIME_LABELS[quote.regime]} · {quote.courier.name}",
    )
    cols[1].metric(
        "Mejor precio en Uruguay",
        money(local_total, rate),
        help=f"en {local_store}" if local_store else None,
    )
    if local_total:
        saving = local_total - quote.total
        cols[2].metric(
            "Ahorro importando" if saving > 0 else "Importar cuesta más",
            money(abs(saving), rate),
            delta=pct(saving / local_total),
        )
    st.caption(
        md(
            f"{REGIME_LABELS[quote.regime]} con {quote.courier.name} · factura "
            f"{money(quote.invoice_usd, rate)} · peso cobrado "
            f"{quote.chargeable_kg:g} kg · llegaría hacia el {settings.arrival:%d/%m/%Y}"
        )
    )
    st.plotly_chart(
        cost_waterfall(
            [line.label for line in quote.lines], [line.amount for line in quote.lines], local_total
        ),
        use_container_width=True,
    )
    for alert in alerts(quote, comparison, settings, cfg, today=today, pool=pool):
        if alert.level == "warning":
            st.warning(md(alert.text), icon=":material/warning:")
        else:
            st.caption(md(f":material/info: {alert.text}"))
    with st.expander("Desglose línea por línea"):
        show_lines(list(quote.lines), rate)
    others = comparison.best_by_courier()
    if len(others) > 1:
        with st.expander("Comparar couriers"):
            st.dataframe(
                pd.DataFrame(
                    {
                        "Courier": [q.courier.name for q in others],
                        "Régimen": [REGIME_LABELS[q.regime] for q in others],
                        "Total": [money(q.total, rate) for q in others],
                        "Plazo (días)": [
                            f"{US_DOMESTIC_DAYS[0] + q.courier.transit_days[0]}–"
                            f"{US_DOMESTIC_DAYS[1] + q.courier.transit_days[1]}"
                            for q in others
                        ],
                        "Tarifa verificada": [f"{q.courier.verified_at:%d/%m/%Y}" for q in others],
                        "Tarifa": [q.courier.source_url for q in others],
                    }
                ),
                hide_index=True,
                column_config={"Tarifa": st.column_config.LinkColumn(display_text="Ver ↗")},
            )


def show_risks(quote: ShipmentQuote | None) -> None:
    with st.expander("Lo que no está en el total: plazo, garantía, devoluciones y requisitos"):
        for risk in risks(quote):
            st.markdown(md(f"{risk.icon} **{risk.title}.** {risk.text}"))


# ---------------------------------------------------------------------------
# Producto page: card and dialog
# ---------------------------------------------------------------------------


@st.dialog("¿Conviene importarlo?", width="large")
def import_dialog(prefill: Prefill, rate) -> None:
    st.markdown(f"**{escape_md(prefill.label)}**")
    with st.expander("Tu situación y supuestos"):
        prefs = prefs_controls(compact=True)
    single_calculator(prefill, prefs, scope="dlg", rate=rate)


def product_section(product: dict, local_usd: float | None, local_store: str | None, rate) -> None:
    """Card of the Producto page: import target price, calculator and US search links."""
    prefill = Prefill(
        key=str(product["id"]),
        label=str(product["name"]),
        category=product.get("category"),
        family_id=product.get("family_id"),
        mpn=product.get("mpn") if isinstance(product.get("mpn"), str) else None,
        local_usd=local_usd,
        local_store=local_store,
    )
    with st.container(border=True):
        st.caption("¿COMPRAR ACÁ O IMPORTAR?")
        if local_usd:
            even, worth = import_targets(
                local_usd, prefill.family_id, prefill.category, current_prefs(), today_local()
            )
            st.markdown(md(targets_text(local_usd, even, worth, rate)))
        else:
            st.markdown("Hoy no se consigue acá: calculá cuánto te sale traerlo.")
        with st.container(horizontal=True):
            if st.button(
                "Calcular si conviene importarlo",
                icon=":material/flight_land:",
                key="imp_open_dialog",
            ):
                import_dialog(prefill, rate)
            if st.button("Agregar al armado", icon=":material/add_shopping_cart:", key="imp_add"):
                add_to_cart(int(product["id"]))
                st.toast("Agregado al armado (página Importar).", icon=":material/check:")


def add_to_cart(product_id: int) -> None:
    ids = list(st.session_state.get("imp_cart_ids", []))
    if product_id not in ids:
        ids.append(product_id)
    st.session_state["imp_cart_ids"] = ids


# ---------------------------------------------------------------------------
# Importar page: one product (catalog, free text or bookmarklet)
# ---------------------------------------------------------------------------


def catalog_products(catalog: pd.DataFrame) -> list[CatalogProduct]:
    return [
        CatalogProduct(
            int(row["id"]),
            str(row["name"]),
            str(row["category"]),
            row["family_id"] if isinstance(row["family_id"], str) else None,
            row["fingerprint"] if isinstance(row["fingerprint"], str) else None,
            row["mpn"] if isinstance(row["mpn"], str) else None,
        )
        for row in catalog.to_dict("records")
    ]


def _best_local(catalog: pd.DataFrame, ids: tuple[int, ...]) -> dict | None:
    """Cheapest current offer among the matched products."""
    rows = [row for row in catalog.to_dict("records") if int(row["id"]) in ids]
    return min(rows, key=lambda row: float(row["best_price_usd"])) if rows else None


def prefill_from_bookmarklet(params: dict[str, str], catalog: pd.DataFrame, rate) -> Prefill | None:
    cfg = load_config()
    intake = parse_intake(params, cfg)
    if intake is None:
        return None
    store = cfg.stores.stores.get(intake.store_id)
    st.markdown(
        md(f"**Desde {store.name if store else 'la tienda'}:** ") + escape_md(intake.title or "")
    )
    for warning in intake.warnings:
        st.warning(md(warning), icon=":material/warning:")
    match: CatalogMatch | None = None
    if intake.title:
        match = match_catalog(intake.title, intake.mpn, catalog_products(catalog))
    best = _best_local(catalog, match.product_ids) if match else None
    local_usd: float | None = None
    local_store = family_id = category = None
    if match and best is not None:
        local_usd, local_store = float(best["best_price_usd"]), str(best["best_store"])
        family_id, category = match.family_id, match.category
        product = escape_md(str(best["name"]))
        if match.is_exact:
            st.success(
                "Es el mismo producto que «"
                + product
                + md(f"»: en Uruguay está a {money(local_usd, rate)} en {local_store}."),
                icon=":material/check_circle:",
            )
        else:
            st.info(
                md(
                    "No está exactamente este modelo; el equivalente más barato de la familia "
                    f"{best['family']} es «"
                )
                + product
                + md(f"» a {money(local_usd, rate)}."),
                icon=":material/compare_arrows:",
            )
        if match.used:
            st.warning("La publicación parece usada o reacondicionada.", icon=":material/warning:")
    else:
        st.info(
            "No encontré este producto en el catálogo de Uruguay: ingresá el precio de acá si lo "
            "conocés.",
            icon=":material/help:",
        )
    return Prefill(
        key="bm",
        label=(intake.title or "Producto")[:120],
        category=category,
        family_id=family_id,
        mpn=intake.mpn,
        local_usd=local_usd,
        local_store=local_store,
        us_price=intake.price_usd,
        store_id=intake.store_id,
        weight_kg=intake.weight_kg,
        dims_cm=intake.dims_cm,
        url=intake.url,
    )


def one_product_tab(prefs: Prefs, catalog: pd.DataFrame, rate) -> None:
    params = {k: st.query_params.get(k) or "" for k in ("t", "p", "u", "sold", "w", "d", "mpn")}
    prefill = None
    if params["t"] or params["p"]:
        prefill = prefill_from_bookmarklet(params, catalog, rate)
    if prefill is None:
        names = {int(i): str(n) for i, n in zip(catalog["id"], catalog["name"], strict=True)}
        prices = {
            int(i): float(p) for i, p in zip(catalog["id"], catalog["best_price_usd"], strict=True)
        }
        choice = st.selectbox(
            "Producto",
            sorted(names, key=lambda i: names[i]),
            index=None,
            format_func=lambda i: f"{names[i]} · {money(prices[i], rate)}" if i in names else i,
            placeholder="Elegí un producto del catálogo o escribí otro",
            accept_new_options=True,
            key="imp_one_choice",
        )
        if choice is None:
            st.caption(
                "También podés mandar un producto desde Amazon, Newegg, B&H o Best Buy con el "
                "botón del navegador (pestaña «Cómo se calcula»)."
            )
            return
        if isinstance(choice, str):
            local = st.number_input(
                "Precio en Uruguay (U$S, opcional)",
                min_value=0.0,
                value=None,
                step=1.0,
                key="imp_one_local",
            )
            prefill = Prefill(key="free", label=choice, local_usd=local or None)
        else:
            row = catalog[catalog["id"] == choice].iloc[0]
            prefill = Prefill(
                key=str(choice),
                label=str(row["name"]),
                category=str(row["category"]),
                family_id=row["family_id"] if isinstance(row["family_id"], str) else None,
                mpn=row["mpn"] if isinstance(row["mpn"], str) else None,
                local_usd=float(row["best_price_usd"]),
                local_store=str(row["best_store"]),
            )
    single_calculator(prefill, prefs, scope="page", rate=rate)


# ---------------------------------------------------------------------------
# Importar page: cart ("Armado")
# ---------------------------------------------------------------------------

CART_COLUMNS = [
    "id",
    "Producto",
    "Cantidad",
    "Precio en Uruguay",
    "Tienda en Uruguay",
    "Tienda en EE. UU.",
    "Precio en EE. UU.",
    "Peso (kg)",
    "Medidas (cm)",
]


def _number(value: object) -> float | None:
    """A positive number from an editor cell (int, float, numpy or text), else None."""
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if number == number and number > 0 else None


def _cart_rows(
    ids: list[int], catalog: pd.DataFrame, previous: pd.DataFrame | None
) -> pd.DataFrame:
    cfg = load_config()
    kept = {}
    if previous is not None and not previous.empty:
        for row in previous.to_dict("records"):
            key = _number(row.get("id"))
            if key is not None and int(key) in ids:
                kept[int(key)] = row
    rows = []
    for product_id in ids:
        if product_id in kept:
            rows.append(kept[product_id])
            continue
        match = catalog[catalog["id"] == product_id]
        if match.empty:
            continue
        product = match.iloc[0]
        family = product["family_id"] if isinstance(product["family_id"], str) else None
        profile = cfg.profiles.for_product(family, str(product["category"]))
        rows.append(
            {
                "id": product_id,
                "Producto": product["name"],
                "Cantidad": 1,
                "Precio en Uruguay": float(product["best_price_usd"]),
                "Tienda en Uruguay": product["best_store"],
                "Tienda en EE. UU.": "amazon",
                "Precio en EE. UU.": None,
                "Peso (kg)": profile.weight_kg,
                "Medidas (cm)": " x ".join(f"{d:g}" for d in profile.dims_cm),
            }
        )
    if previous is not None and not previous.empty:  # free rows added by the user
        rows.extend(row for row in previous.to_dict("records") if _number(row.get("id")) is None)
    return pd.DataFrame(rows, columns=CART_COLUMNS)


def _parse_dims(text: object) -> tuple[float, float, float] | None:
    numbers = re.findall(r"\d+(?:[.,]\d+)?", str(text or ""))
    if len(numbers) != 3:
        return None
    a, b, c = (float(n.replace(",", ".")) for n in numbers)
    return a, b, c


def cart_items(table: pd.DataFrame) -> tuple[list[CartItem], list[str]]:
    items, problems = [], []
    for row in table.to_dict("records"):
        label = str(row.get("Producto") or "").strip()
        if not label:
            continue
        qty = int(_number(row.get("Cantidad")) or 1)
        local = _number(row.get("Precio en Uruguay"))
        us_price = _number(row.get("Precio en EE. UU."))
        foreign = None
        if us_price is not None:
            foreign = ForeignItem(
                label,
                us_price,
                store_id=str(row.get("Tienda en EE. UU.") or "amazon"),
                unit_weight_kg=_number(row.get("Peso (kg)")) or 0.5,
                unit_dims_cm=_parse_dims(row.get("Medidas (cm)")),
            )
        if foreign is None and local is None:
            problems.append(label)
            continue
        store = row.get("Tienda en Uruguay")
        items.append(
            CartItem(
                label, max(qty, 1), local, str(store) if isinstance(store, str) else None, foreign
            )
        )
    return items, problems


def cart_tab(prefs: Prefs, catalog: pd.DataFrame, rate) -> None:
    cfg = load_config()
    state = st.session_state
    names = {int(i): str(n) for i, n in zip(catalog["id"], catalog["name"], strict=True)}
    st.markdown(
        "Elegí los productos y completá el precio en EE. UU. de cada uno. El plan decide qué "
        "conviene comprar acá, qué importar y cómo agrupar los envíos para aprovechar las "
        "franquicias y el tope de U$S 200 sin IVA."
    )
    selected = st.multiselect(
        "Productos del catálogo",
        sorted(names, key=lambda i: names[i]),
        placeholder="Elegí uno o más productos",
        default=[i for i in state.get("imp_cart_ids", []) if i in names],
        format_func=lambda i: names.get(i, str(i)),
        key="w_imp_cart_ids",
    )
    # The editor applies its edits on top of the table it receives, so that table
    # only changes when the selection changes (new editor key) or when coming back
    # to the page (the editor state is gone and the last edited table is the base).
    version = state.get("imp_cart_version", 0)
    if selected != state.get("imp_cart_ids_built") or "imp_cart_base" not in state:
        state["imp_cart_base"] = _cart_rows(selected, catalog, state.get("imp_cart_last"))
        state["imp_cart_ids_built"] = selected
        version += 1
        state["imp_cart_version"] = version
    elif f"w_imp_cart_{version}" not in state and state.get("imp_cart_last") is not None:
        state["imp_cart_base"] = state["imp_cart_last"]
    state["imp_cart_ids"] = selected
    stores = list(STORE_CHOICES)
    edited = st.data_editor(
        state["imp_cart_base"],
        key=f"w_imp_cart_{version}",
        num_rows="dynamic",
        hide_index=True,
        column_order=CART_COLUMNS[1:],
        column_config={
            "Producto": st.column_config.TextColumn(required=True),
            "Cantidad": st.column_config.NumberColumn(min_value=1, max_value=MAX_UNITS, step=1),
            "Precio en Uruguay": st.column_config.NumberColumn(format="U$S %.2f", min_value=0),
            "Tienda en Uruguay": st.column_config.TextColumn(),
            "Tienda en EE. UU.": st.column_config.SelectboxColumn(
                options=stores, required=True, default="amazon"
            ),
            "Precio en EE. UU.": st.column_config.NumberColumn(format="U$S %.2f", min_value=0),
            "Peso (kg)": st.column_config.NumberColumn(min_value=0.01, step=0.05),
            "Medidas (cm)": st.column_config.TextColumn(help="Largo x ancho x alto"),
        },
    )
    state["imp_cart_last"] = edited
    with st.expander("Tiendas de EE. UU. (códigos de la tabla)"):
        st.markdown("\n".join(f"- `{s}`: {md(cfg.stores.stores[s].name)}" for s in STORE_CHOICES))
    hassle = st.number_input(
        "¿Cuánto te molesta cada envío extra? (U$S)",
        min_value=0.0,
        value=0.0,
        step=1.0,
        key="w_imp_hassle",
        help="Con 0, el plan separa envíos siempre que ahorre aunque sea un dólar.",
    )
    if not st.button("Calcular el mejor plan", type="primary", icon=":material/calculate:"):
        return
    items, problems = cart_items(edited)
    for label in problems:
        st.warning(
            md(f"«{label}»: falta el precio en Uruguay o en EE. UU."), icon=":material/edit:"
        )
    if not items:
        return
    units = sum(item.qty for item in items)
    if units > MAX_UNITS:
        st.error(f"El armado admite hasta {MAX_UNITS} unidades (tenés {units}).")
        return
    today = today_local()
    settings = prefs.settings(today, cfg)
    pools = prefs.pools(cfg, settings.arrival, today)
    with st.spinner("Buscando la mejor combinación…"):
        result = optimize(items, pools, settings, cfg, hassle_usd=hassle)
        alone = (
            optimize(items, pools[:1], settings, cfg, hassle_usd=hassle) if len(pools) > 1 else None
        )
    show_plan(result, alone, prefs, rate)


def show_plan(result: CartResult, alone: CartResult | None, prefs: Prefs, rate) -> None:
    plan = result.best
    for unit in result.unplaced:
        st.error(
            "«"
            + escape_md(unit.item.label)
            + md("» no se consigue acá y no entra por courier (más de U$S 800 o 20 kg)."),
            icon=":material/block:",
        )
    if plan is None:
        st.error("No hay un plan posible con estos datos.")
        return
    cols = st.columns(3)
    cols[0].metric("Todo en Uruguay", money(result.all_local, rate))
    cols[1].metric("Plan óptimo", money(plan.total, rate), help=f"Envíos con {plan.courier.name}")
    if result.all_local:
        saving = result.all_local - plan.total
        cols[2].metric("Ahorro", money(saving, rate), delta=pct(saving / result.all_local))
    if alone and alone.best and alone.best.total > plan.total:
        family_saving = money(alone.best.total - plan.total, rate)
        st.caption(
            md(f"Las franquicias de tu familia ahorran {family_saving} frente a usar solo la tuya.")
        )

    # Labels come from the user (free rows): escape them; md() only for our text.
    steps = []
    if plan.local:
        listing = ", ".join(
            escape_md(unit.item.label)
            + md(
                f" ({unit.item.local_store or 'tienda local'}, {money(unit.item.local_usd, rate)})"
            )
            for unit in plan.local
        )
        steps.append(f"**Comprá en Uruguay:** {listing}.")
    cfg = load_config()
    for number, shipment in enumerate(plan.shipments, start=1):
        quote = shipment.quote
        if shipment.holder:
            regime = f"franquicia de {escape_md(shipment.holder)}"
            regime += ", sin IVA" if quote.exempt else ", con IVA"
        else:
            regime = "régimen del 60%"
        stores = ", ".join(sorted({cfg.stores.stores[i.store_id].name for i in quote.items}))
        steps.append(
            f"**Envío {number}** ({regime}, factura {md(money(quote.invoice_usd, rate))}): "
            + escape_md(shipment.units_label)
            + md(
                f". Pedido aparte en {stores}, por {quote.courier.name}. Puesto acá: "
                f"{money(quote.total, rate)}."
            )
        )
    st.markdown("\n".join(f"{i}. {step}" for i, step in enumerate(steps, start=1)))
    if any(s.holder and s.holder != prefs.people[0].name for s in plan.shipments):
        st.info(
            "Cada familiar compra con su tarjeta internacional a su nombre, el paquete viene a "
            "su nombre y tiene que estar registrado en Aduanas. Es su franquicia: uso personal, "
            "sin fines comerciales.",
            icon=":material/family_restroom:",
        )
    st.caption(
        "Hacé un pedido separado por envío: cada envío necesita su propia factura. Si la tienda "
        "manda todo junto, pedile al courier que no los consolide."
    )
    for note in plan.notes:
        st.warning(md(note), icon=":material/warning:")
    for number, shipment in enumerate(plan.shipments, start=1):
        with st.expander(f"Desglose del envío {number}"):
            show_lines(list(shipment.quote.lines), rate)
    if len(result.by_courier) > 1:
        with st.expander("El mismo armado con cada courier"):
            st.dataframe(
                pd.DataFrame(
                    {
                        "Courier": [p.courier.name for p in result.by_courier],
                        "Total": [money(p.total, rate) for p in result.by_courier],
                        "Envíos": [len(p.shipments) for p in result.by_courier],
                    }
                ),
                hide_index=True,
            )
    show_risks(plan.shipments[0].quote if plan.shipments else None)


# ---------------------------------------------------------------------------
# Importar page: how it is computed and the bookmarklet
# ---------------------------------------------------------------------------


def app_url() -> str:
    url = getattr(st.context, "url", None)
    if url:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}"
    return APP_URL


def bookmarklet_href(base_url: str) -> str:
    lines = [
        line
        for line in BOOKMARKLET_PATH.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("//")
    ]
    source = "\n".join(lines).replace("__APP_URL__", base_url.rstrip("/"))
    return "javascript:" + urllib.parse.quote(source, safe="")


def how_tab() -> None:
    cfg = load_config()
    rules = cfg.rules.for_date(today_local())
    franchise, treaty, single = rules.franchise, rules.treaty, rules.single_tax
    st.markdown(
        md(
            f"""
**Reglas vigentes** ({rules.name}, verificadas el {rules.verified_at:%d/%m/%Y})

- **Franquicia:** hasta {franchise.max_shipments} envíos por año civil y U$S
  {franchise.annual_usd_cap:g} en total por persona mayor de edad, para uso personal,
  pagando con tarjeta internacional a tu nombre. No paga aranceles pero paga **IVA del
  {pct(franchise.iva_rate, signed=False)}** sobre el valor de la factura, con un mínimo de U$S
  {franchise.iva_min_usd:g} por envío.
- **Sin IVA desde EE. UU.:** si todo lo vende una empresa de EE. UU. que figura en el
  registro de Aduanas (Amazon.com, Newegg, B&H, Best Buy, Micro Center…) y el envío vale
  hasta U$S {treaty.max_invoice_usd:g}. Desde el {treaty.registry_required_from:%d/%m/%Y}
  el registro es obligatorio. Igual usa uno de tus envíos y suma a los U$S
  {franchise.annual_usd_cap:g}.
- **Prestación única:** {pct(single.rate, signed=False)} del valor de la factura (mínimo U$S
  {single.min_usd:g}), sin límite de envíos. Si un envío no entra en tu franquicia, va
  entero a este régimen: no se combinan.
- **Topes por envío:** U$S {single.max_invoice_usd:g} y {single.max_weight_kg:g} kg. Más que
  eso es una importación formal con despachante (no se calcula acá).
- **El valor es el total de la factura de la tienda:** precio, sales tax (7% en Miami) y
  envío hasta el depósito. El flete del courier no cuenta.
- **El courier cobra aparte:** flete por tramos de peso, gestión, la tasa postal TFSPU (10%)
  y, si lo pedís, seguro y entrega.
"""
        )
    )
    st.markdown("**Fuentes**")
    st.markdown("\n".join(f"- [{s.label}]({s.url})" for s in rules.sources.values()))
    st.markdown("**Couriers incluidos**")
    st.dataframe(
        pd.DataFrame(
            {
                "Courier": [c.name for c in cfg.couriers.values()],
                "Gestión (U$S)": [c.handling_usd for c in cfg.couriers.values()],
                "Depósito": [c.warehouse for c in cfg.couriers.values()],
                "Exención de sales tax": [
                    "Sí" if c.tax_free_certificate else "—" for c in cfg.couriers.values()
                ],
                "Verificada": [f"{c.verified_at:%d/%m/%Y}" for c in cfg.couriers.values()],
                "Tarifa": [c.source_url for c in cfg.couriers.values()],
            }
        ),
        hide_index=True,
        column_config={"Tarifa": st.column_config.LinkColumn(display_text="Ver ↗")},
    )
    st.markdown("**Mandá un producto desde la tienda de EE. UU.**")
    st.markdown(
        "Arrastrá este botón a la barra de favoritos de tu navegador. Cuando estés en la ficha "
        "de un producto en Amazon, Newegg, B&H o Best Buy, hacé clic en el favorito: se abre "
        "esta calculadora con el precio, el vendedor, el peso y las medidas. Corre solo en tu "
        "navegador y no guarda nada."
    )
    href = bookmarklet_href(app_url())
    st_components.html(
        f"""<a href="{html_lib.escape(href, quote=True)}"
             onclick="alert('Arrastrá este botón a tu barra de favoritos.'); return false;"
             style="display:inline-block;padding:.5rem .9rem;border-radius:.5rem;
                    background:#2a78d6;color:#fff;font:600 14px sans-serif;
                    text-decoration:none">Hardware Pulse: ¿importar?</a>""",
        height=52,
    )
    with st.expander("¿No se puede arrastrar? Copiá el código"):
        st.caption("Creá un favorito nuevo y pegá esto como dirección (URL).")
        st.code(href, language=None, wrap_lines=True)
