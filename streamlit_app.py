"""
Hardware Pulse — comparador de precios de hardware para Uruguay.

    streamlit run streamlit_app.py
"""

from __future__ import annotations

import streamlit as st

from src.dashboard import data
from src.dashboard.components import freshness_banner, inject_css
from src.dashboard.formatting import CURRENCIES, md

st.set_page_config(
    page_title="Hardware Pulse · precios de hardware en Uruguay",
    page_icon=":material/memory:",
    layout="wide",
)
inject_css()

PAGES = "src/dashboard/pages"
navigation = st.navigation(
    {
        "Precios": [
            st.Page(f"{PAGES}/inicio.py", title="Inicio", icon=":material/home:", default=True),
            st.Page(f"{PAGES}/catalogo.py", title="Catálogo", icon=":material/storefront:"),
            st.Page(f"{PAGES}/familias.py", title="Familias", icon=":material/category:"),
            st.Page(
                f"{PAGES}/producto.py",
                title="Producto",
                icon=":material/sell:",
                visibility="hidden",
            ),
        ],
        "Herramientas": [
            st.Page(
                f"{PAGES}/importar.py",
                title="¿Comprar acá o importar?",
                icon=":material/flight_land:",
                url_path="importar",
            ),
        ],
        "Transparencia": [
            st.Page(f"{PAGES}/estado.py", title="Estado de datos", icon=":material/monitor_heart:"),
            st.Page(
                f"{PAGES}/modelo.py", title="Señal comprar/esperar", icon=":material/insights:"
            ),
        ],
    }
)

with st.sidebar:
    st.segmented_control(
        "Moneda",
        CURRENCIES,
        key="currency",
        default="USD",
        required=True,
        format_func=lambda c: "Dólares (U$S)" if c == "USD" else "Pesos ($)",
    )
    rate = data.usd_uyu()
    if rate:
        st.caption(md(f"Cotización usada: 1 U$S = $ {rate:.2f}".replace(".", ",")))
    freshness_banner()
    st.caption("Precios publicados por cada tienda, en dólares con IVA incluido.")

navigation.run()
