"""Familias: market view by chip/model (cheapest model, median, 30-day trend)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import data
from src.dashboard.components import RANGES, category_label, family_chart, product_card
from src.dashboard.formatting import currency, money
from src.domain.models import Category

rate = data.usd_uyu()
st.title("Familias")
st.markdown(
    "Una familia agrupa todos los modelos del mismo chip o especificación (por ejemplo, todas "
    "las **RTX 5060 Ti 16GB** de cualquier marca). Sirve para ver cuánto cuesta entrar a ese "
    "nivel de rendimiento y cómo se mueve el mercado."
)

families = data.families()
cat = st.pills(
    "Categoría", [c.value for c in Category], format_func=category_label, default=Category.GPU.value
)
view = families[families["category"] == cat] if cat else families

if view.empty:
    st.info("Sin familias con precio en esta categoría.")
    st.stop()

view = view.copy()
per_gb = view["specs"].apply(lambda s: s.get("capacity_gb"))
if cat in ("ram", "ssd"):
    view["usd_per_gb"] = view["min_usd"] / per_gb
factor = rate if currency() == "UYU" and rate else 1.0
table = pd.DataFrame(
    {
        "Familia": view["name"],
        "Desde": view["min_usd"] * factor,
        "Modelos": view["n_products"],
        "Ofertas": view["n_offers"],
        "Cambio 30 días": view["change_30d"] * 100,
        "Tendencia 90 días": view["sparkline"],
    }
)
if "usd_per_gb" in view:
    table["U$S por GB"] = view["usd_per_gb"].round(2)
prefix = "$ " if factor != 1.0 else "U$S "
selection = st.dataframe(
    table.sort_values("Desde"),
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "Desde": st.column_config.NumberColumn(format=f"{prefix}%,.0f"),
        "Cambio 30 días": st.column_config.NumberColumn(format="%+.1f%%"),
        "Tendencia 90 días": st.column_config.LineChartColumn(y_min=None, y_max=None),
    },
    key="families_table",
)
st.caption("Elegí una fila para ver el detalle de la familia.")

rows = selection.selection.rows if selection else []
if rows:
    family = view.sort_values("min_usd").iloc[rows[0]]
    st.subheader(family["name"])
    daily = data.family_daily(family["id"])
    if not daily.empty:
        choice = st.segmented_control("Rango", list(RANGES), default="90 días", key="fam_range")
        st.plotly_chart(family_chart(daily, RANGES[choice or "90 días"]), use_container_width=True)
    products = data.family_products(family["id"])
    st.markdown(
        f"**{len(products)} modelos** desde {money(products['best_price_usd'].min(), rate)}"
    )
    grid = st.columns(4)
    for i, (_, row) in enumerate(products.head(12).iterrows()):
        with grid[i % 4]:
            product_card(row, rate)
