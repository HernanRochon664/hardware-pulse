"""Producto: best offer, every store's offer, price history and the buy/wait signal."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import data
from src.dashboard.components import RANGES, badges, category_label, history_chart, product_card
from src.dashboard.formatting import ago, money, pct, short_date

CONDITION_LABELS = {"open_box": "Open box", "used": "Usado", "refurbished": "Reacondicionado"}
SPEC_LABELS = {
    "chip": "Chip",
    "vram_gb": "Memoria de video (GB)",
    "vendor": "Fabricante",
    "model": "Modelo",
    "generation": "Tipo",
    "capacity_gb": "Capacidad (GB)",
    "speed_mts": "Velocidad (MT/s)",
    "form": "Formato",
    "kit": "Kit",
    "interface": "Interfaz",
    "modifiers": "Variante",
    "variants": "Variante",
    "rgb": "RGB",
    "white": "Blanca",
}

rate = data.usd_uyu()
raw_id = st.query_params.get("p")
product = data.product(int(raw_id)) if raw_id and raw_id.isdigit() else None
if product is None:
    st.title("Producto no encontrado")
    st.page_link(
        "src/dashboard/pages/catalogo.py", label="Volver al catálogo", icon=":material/arrow_back:"
    )
    st.stop()

st.page_link("src/dashboard/pages/catalogo.py", label="Catálogo", icon=":material/arrow_back:")
st.title(product["name"])
st.caption(f"{category_label(product['category'])} · Familia: {product.get('family') or '—'}")

offers = data.offers(int(product["id"]))
current = offers[offers["is_current"] == 1]
others_new = offers[(offers["is_current"] != 1) & (offers["condition"] == "new")]
second_hand = offers[offers["condition"] != "new"]

left, right = st.columns([1, 2])
with left:
    if isinstance(product.get("image_url"), str) and product["image_url"]:
        st.image(product["image_url"], width=260)
with right:
    if current.empty:
        st.warning("Hoy ninguna tienda lo tiene disponible.", icon=":material/inventory_2:")
    else:
        best = current.iloc[0]
        with st.container(border=True):
            st.caption("PRECIO MÁS BAJO")
            before = ""
            if pd.notna(best["regular_price"]) and best["regular_price"] > best["price"]:
                regular_usd = best["price_usd"] * best["regular_price"] / best["price"]
                before = f"<span class='hp-before'>{money(regular_usd, rate)}</span>"
            st.markdown(
                f"<div class='hp-price'>{money(best['price_usd'], rate)}{before}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(f"en **{best['store']}** · visto {ago(best['last_seen_at'])}")
            st.link_button(
                f"Ver en {best['store']}",
                best["url"],
                icon=":material/open_in_new:",
                type="primary",
            )
            if len(current) > 1:
                spread = current["price_usd"].max() - best["price_usd"]
                st.caption(f"{len(current)} ofertas · la más cara cuesta {money(spread, rate)} más")
        with st.container(horizontal=True):
            badges(product)

    signal = data.signal(int(product["id"]))
    if signal:
        verdict = (
            "Esperar"
            if signal["recommendation"] == "esperar"
            else (
                "Buen momento para comprar"
                if signal["recommendation"] == "comprar"
                else "Sin señal clara"
            )
        )
        st.info(
            f"**Señal: {verdict}** · probabilidad de que baje 5% o más en los próximos "
            f"{signal['horizon_days']} días: **{pct(signal['p_drop'], signed=False)}**. "
            "Es una estimación del modelo, no una garantía.",
            icon=":material/insights:",
        )

st.subheader("Ofertas")
if offers.empty:
    st.info("Sin ofertas registradas.")
else:
    table = pd.DataFrame(
        {
            "Tienda": offers["store"],
            "Precio": offers["price_usd"].map(lambda v: money(v, rate)),
            "Antes": [
                money(r.price_usd * r.regular_price / r.price, rate)
                if pd.notna(r.regular_price) and r.regular_price > r.price
                else ""
                for r in offers.itertuples()
            ],
            "Estado": [
                CONDITION_LABELS.get(r.condition)
                or (
                    "Disponible"
                    if r.is_current == 1
                    else "Sin stock"
                    if r.in_stock == 0
                    else "Precio dudoso"
                    if isinstance(r.quality_issue, str)
                    else "Ya no publicado"
                    if r.is_active == 0
                    else "Desactualizado"
                )
                for r in offers.itertuples()
            ],
            "Visto": offers["last_seen_at"].map(ago),
            "Publicación en la tienda": offers["title"],
            "Link": offers["url"],
        }
    )
    st.dataframe(
        table,
        hide_index=True,
        column_config={"Link": st.column_config.LinkColumn("Link", display_text="Ver ↗")},
    )
    if not second_hand.empty:
        st.caption(
            "Las ofertas open box, usadas o reacondicionadas no cuentan para el mejor precio."
        )

st.subheader("Historial de precios")
periods = data.price_periods(int(product["id"]))
daily = data.product_daily(int(product["id"]))
if daily.empty:
    st.info("Todavía no hay historial para este producto.")
else:
    choice = st.segmented_control("Rango", list(RANGES), default="180 días", key="range")
    days = RANGES[choice or "180 días"]
    st.plotly_chart(history_chart(periods, daily, days), use_container_width=True)
    stats = st.columns(4)
    stats[0].metric("Mínimo (180 días)", money(product.get("min_180d"), rate))
    stats[1].metric("Máximo (180 días)", money(product.get("max_180d"), rate))
    stats[2].metric("Cambio 30 días", pct(product.get("change_30d")))
    stats[3].metric("Último cambio", short_date(product.get("last_change_at")))
    if (periods["origin"] == "legacy").any():
        st.caption(
            "El historial anterior al 15/10/2026 proviene de la versión 1 del proyecto "
            "(3 tiendas, sin datos de stock) y es aproximado."
        )

specs = {
    SPEC_LABELS.get(k, k): v for k, v in product["specs"].items() if v not in (None, [], False)
}
if specs:
    st.subheader("Especificaciones")
    st.table(
        pd.DataFrame(
            {
                "": [
                    ", ".join(map(str, v)) if isinstance(v, list) else ("Sí" if v is True else v)
                    for v in specs.values()
                ]
            },
            index=list(specs),
        )
    )

if product.get("family_id"):
    alternatives = data.family_products(product["family_id"])
    alternatives = alternatives[alternatives["id"] != product["id"]].head(4)
    if not alternatives.empty:
        st.subheader(f"Otros modelos de {product['family']}")
        grid = st.columns(4)
        for i, (_, row) in enumerate(alternatives.iterrows()):
            with grid[i % 4]:
                product_card(row, rate)

if not others_new.empty:
    with st.expander("Publicaciones sin stock o desactualizadas"):
        st.dataframe(others_new[["store", "price_usd", "last_seen_at", "title"]], hide_index=True)
