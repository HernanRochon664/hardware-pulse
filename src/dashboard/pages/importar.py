"""Importar: "¿comprar acá o importar?" for one product, a cart (armado) and the rules."""

from __future__ import annotations

import streamlit as st

from src.dashboard import data, importing

rate = data.usd_uyu()
catalog = data.catalog()

st.title("¿Comprar acá o importar?")
st.caption(
    "Compará el mejor precio de Uruguay con lo que realmente cuesta traerlo de EE. UU.: "
    "sales tax, courier, tasas, IVA y franquicias, línea por línea y con la fuente de cada número."
)

with st.expander("Tu situación: franquicia usada este año, courier, tarjeta y umbral de ahorro"):
    prefs = importing.prefs_controls()

tab_one, tab_cart, tab_how = st.tabs(
    ["Un producto", "Armado (varios productos)", "Cómo se calcula"]
)
with tab_one:
    importing.one_product_tab(prefs, catalog, rate)
with tab_cart:
    importing.cart_tab(prefs, catalog, rate)
with tab_how:
    importing.how_tab()
