"""Inicio: search, headline numbers, today's price drops and historic lows."""

from __future__ import annotations

import streamlit as st

from src.dashboard import data
from src.dashboard.components import product_card
from src.domain.models import CATEGORY_LABELS

st.title("Hardware Pulse")
st.markdown(
    "Compará precios de **tarjetas de video, procesadores, memorias, SSD, motherboards, "
    "coolers, fuentes, gabinetes y monitores** en las tiendas de Uruguay. Cada producto "
    "agrupa las ofertas de todas las tiendas que lo venden."
)

catalog = data.catalog()
status = data.status()
rate = data.usd_uyu()

query = st.text_input(
    "Buscar",
    placeholder="Ej: rtx 5060 ti, ryzen 7 7800x3d, ddr5 32gb, b650m, 27 qhd ips",
    label_visibility="collapsed",
)
if query:
    st.switch_page("src/dashboard/pages/catalogo.py", query_params={"q": query})

with st.container(horizontal=True):
    for category, label in CATEGORY_LABELS.items():
        st.page_link(
            "src/dashboard/pages/catalogo.py",
            label=label,
            icon=":material/arrow_forward:",
            query_params={"cat": category.value},
        )

cols = st.columns(4)
cols[0].metric("Productos con precio", f"{len(catalog):,}".replace(",", "."))
n_offers = sum(int(v) for v in catalog["n_offers"].tolist() if v == v)
cols[1].metric("Ofertas comparadas", f"{n_offers:,}".replace(",", "."))
cols[2].metric("Tiendas al día", f"{status['stores_ok']} de {status['stores_total']}")
cols[3].metric(
    "Comparables en 2+ tiendas", f"{int((catalog['n_stores'] > 1).sum()):,}".replace(",", ".")
)

drops = catalog.loc[catalog["change_7d"] <= -0.03].sort_values(by=["change_7d"]).head(8)
lows = catalog.loc[catalog["is_historic_low"] == 1].sort_values(by=["change_30d"]).head(8)

st.subheader("Bajaron en los últimos 7 días")
if drops.empty:
    st.info("Esta semana no hubo bajas de precio de 3% o más.")
else:
    grid = st.columns(4)
    for i, row in enumerate(drops.to_dict(orient="records")):
        with grid[i % 4]:
            product_card(row, rate)

st.subheader("En su mínimo histórico")
if lows.empty:
    st.info("Ningún producto está hoy en su precio más bajo registrado.")
else:
    grid = st.columns(4)
    for i, row in enumerate(lows.to_dict(orient="records")):
        with grid[i % 4]:
            product_card(row, rate)
