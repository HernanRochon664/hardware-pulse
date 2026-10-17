"""Catálogo: preciospc-style filters, sorting and a paginated grid of product cards."""

from __future__ import annotations

import math

import pandas as pd
import streamlit as st

from src.dashboard import data
from src.dashboard.components import category_label, product_card
from src.dashboard.formatting import column_range, md, money
from src.domain.models import Category
from src.entities.text import fold

SORTS = {
    "Precio: menor a mayor": ("best_price_usd", True),
    "Precio: mayor a menor": ("best_price_usd", False),
    "Mayor baja en 30 días": ("change_30d", True),
    "Más tiendas": ("n_stores", False),
    "Nombre A–Z": ("name", True),
}
PAGE_SIZES = (24, 48, 96)
# Spec filters of the categories added in 2026-10 (the first four keep the
# family filter only). key -> label; values come from the products' specs.
CATEGORY_FILTERS = {
    "monitor": {
        "panel": "Panel",
        "resolution": "Resolución",
        "refresh_hz": "Frecuencia (Hz)",
        "size_class": "Tamaño (pulgadas)",
    },
    "motherboard": {
        "socket": "Socket",
        "chipset": "Chipset",
        "form": "Formato",
        "memory": "Memoria",
    },
    "cooler": {"kind": "Tipo", "radiator_mm": "Radiador (mm)"},
    "psu": {"wattage_w": "Potencia (W)", "rating": "Certificación", "modular": "Modular"},
}


def spec_value(specs: dict, key: str):
    if key == "size_class":  # 23.8" and 24" are the same size class
        size = specs.get("size_in")
        return int(size + 0.5) if isinstance(size, int | float) else None
    return specs.get(key)


params = st.query_params
catalog = data.catalog()
rate = data.usd_uyu()
store_names = dict(zip(data.stores()["id"], data.stores()["name"], strict=True))

st.title("Catálogo")

query = st.text_input("Buscar", value=params.get("q", ""), placeholder="Modelo, marca o familia")
with st.sidebar:
    st.subheader("Filtros")
    categories = [c.value for c in Category]
    cat = st.pills(
        "Categoría",
        categories,
        format_func=category_label,
        default=params.get("cat") if params.get("cat") in categories else None,
    )
    pool: pd.DataFrame = catalog if not cat else catalog.loc[catalog["category"] == cat]
    family_names = sorted(pool["family"].dropna().unique())
    families = st.multiselect(
        "Familia / chip",
        family_names,
        default=[f for f in params.get_all("fam") if f in family_names],
    )
    spec_filters: dict[str, list] = {}
    for key, label in CATEGORY_FILTERS.get(cat or "", {}).items():
        values = {spec_value(s, key) for s in pool["specs"]} - {None}
        if values:
            spec_filters[key] = st.multiselect(
                label, sorted(values, key=lambda v: (str(type(v)), v))
            )
    brands = st.multiselect("Marca", sorted(pool["brand"].dropna().unique()))
    stores = st.multiselect(
        "Tienda con el mejor precio",
        sorted(store_names),
        format_func=lambda s: store_names.get(s, s),
    )
    multi_store = st.toggle("Solo comparables en 2+ tiendas", value=False)
    deals = st.toggle("Solo en mínimo histórico o en baja", value=False)
    if not pool.empty:
        lo, hi = column_range(pool, "best_price_usd")
        lo, hi = math.floor(lo), math.ceil(hi)
        price_range = st.slider("Precio (U$S)", lo, max(hi, lo + 1), (lo, max(hi, lo + 1)))
    else:
        price_range = (0, 0)

df: pd.DataFrame = pool
if query:
    terms = fold(query).split()
    haystack = (
        df["name"].fillna("") + " " + df["family"].fillna("") + " " + df["brand"].fillna("")
    ).map(fold)
    df = df.loc[haystack.apply(lambda text: all(t in text for t in terms))]
if families:
    df = df.loc[df["family"].isin(families)]
for key, chosen in spec_filters.items():
    if chosen:
        df = df.loc[df["specs"].apply(lambda s, k=key, c=chosen: spec_value(s, k) in c)]
if brands:
    df = df.loc[df["brand"].isin(brands)]
if stores:
    df = df.loc[df["best_store_id"].isin(stores)]
if multi_store:
    df = df.loc[df["n_stores"] > 1]
if deals:
    df = df.loc[(df["is_historic_low"] == 1) | (df["change_7d"] <= -0.03)]
df = df.loc[(df["best_price_usd"] >= price_range[0]) & (df["best_price_usd"] <= price_range[1])]

top = st.columns([2, 1, 1])
sort = top[1].selectbox("Ordenar", list(SORTS))
size = top[2].selectbox("Por página", PAGE_SIZES)
column, ascending = SORTS[sort]
df = df.sort_values(by=[column], ascending=ascending, na_position="last")

pages = max(1, math.ceil(len(df) / size))
top[0].markdown(
    md(
        f"**{len(df)} {'producto' if len(df) == 1 else 'productos'}**"
        + (f" · desde {money(column_range(df, 'best_price_usd')[0], rate)}" if len(df) else "")
    )
)

if df.empty:
    st.info("No hay productos con esos filtros. Probá quitar alguno.")
else:
    page = st.number_input("Página", 1, pages, 1) if pages > 1 else 1
    view = df.iloc[(page - 1) * size : page * size]
    grid = st.columns(4)
    for i, row in enumerate(view.to_dict(orient="records")):
        with grid[i % 4]:
            product_card(row, rate)
    if pages > 1:
        st.caption(f"Página {page} de {pages}")
