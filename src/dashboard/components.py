"""Reusable dashboard pieces: styles, product cards, badges and charts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard import data
from src.dashboard.formatting import ago, html, money, pct
from src.domain.models import CATEGORY_LABELS, Category
from src.settings import LOCAL_TZ, from_iso

# Reference categorical palette (validated light/dark, see docs/decisions.md).
SERIES_1 = "#2a78d6"  # blue: the lowest price, the line that matters
SERIES_2 = "#eb6834"  # orange: family median
MUTED = "#898781"  # per-store lines and context
GRID = "rgba(137,135,129,0.25)"

CSS = """
<style>
.hp-price {font-size: 1.6rem; font-weight: 700; line-height: 1.1}
.hp-price-small {font-size: 1.25rem; font-weight: 700}
.hp-before {text-decoration: line-through; opacity: .65; margin-left: .4rem}
.hp-muted {opacity: .7; font-size: .85rem}
.hp-name {font-weight: 600; line-height: 1.25; min-height: 2.5em}
.hp-card img {border-radius: 6px}
</style>
"""


def inject_css() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def category_label(value: str) -> str:
    return CATEGORY_LABELS.get(Category(value), value) if value else ""


def freshness_banner() -> None:
    status = data.status()
    last = status.get("last_scrape_at")
    ok, total = status.get("stores_ok", 0), status.get("stores_total", 0)
    text = f"Precios actualizados {ago(last)} · {ok}/{total} tiendas al día"
    if last and datetime.now(UTC) - from_iso(last) > timedelta(hours=30):
        st.warning(text + " · los datos pueden estar desactualizados", icon=":material/schedule:")
    elif total and ok < total:
        st.caption(f":material/warning: {text}")
    else:
        st.caption(f":material/check_circle: {text}")


def badges(row: Mapping[str, Any]) -> None:
    """Deal badges: always icon + text, never color alone."""
    if row.get("is_historic_low"):
        st.badge("Mínimo histórico", icon=":material/trending_down:", color="green")
    change = row.get("change_7d")
    if change is not None and change == change and change <= -0.03:
        st.badge(
            f"Bajó {pct(-change, signed=False)} en 7 días",
            icon=":material/arrow_downward:",
            color="blue",
        )
    regular, best = row.get("best_regular_usd"), row.get("best_price_usd")
    if regular and best and regular == regular and regular > best:
        st.badge(
            f"Promo {pct((regular - best) / regular, signed=False)}",
            icon=":material/sell:",
            color="orange",
        )
    rec = row.get("recommendation")
    if rec == "esperar":
        st.badge("Señal: esperar", icon=":material/hourglass_top:", color="violet")
    elif rec == "comprar":
        st.badge("Señal: buen momento", icon=":material/shopping_cart:", color="green")


def product_card(row: Mapping[str, Any], rate: float | None) -> None:
    with st.container(border=True):
        image = row.get("image_url")
        if isinstance(image, str) and image:
            st.image(image, width=140)
        st.caption(
            f"{row.get('brand') or ''} · {row.get('family') or category_label(row['category'])}"
        )
        st.page_link(
            "src/dashboard/pages/producto.py",
            label=row["name"],
            query_params={"p": str(int(row["id"]))},
        )
        st.markdown(
            html(f"<div class='hp-price-small'>{money(row['best_price_usd'], rate)}</div>"),
            unsafe_allow_html=True,
        )
        extra = f" · +{int(row['n_stores']) - 1} tiendas" if row["n_stores"] > 1 else ""
        st.caption(f"en {row['best_store']}{extra}")
        with st.container(horizontal=True, gap="small"):
            badges(row)


def _local(stamps: pd.Series) -> pd.Series:
    return pd.to_datetime(stamps, utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)


def _layout(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        height=height,
        margin={"l": 8, "r": 8, "t": 24, "b": 8},
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        yaxis={"title": None, "tickprefix": "U$S ", "gridcolor": GRID, "zeroline": False},
        xaxis={"title": None, "showgrid": False},
        separators=",.",
    )
    return fig


def history_chart(periods: pd.DataFrame, daily: pd.DataFrame, days: int | None) -> go.Figure:
    """Lowest price (step line) plus each store's price history in a muted tone."""
    fig = go.Figure()
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days) if days else None
    for store, group in periods.loc[periods["condition"] == "new"].groupby("store"):
        xs, ys = [], []
        for row in group.to_dict(orient="records"):
            start, end = _local(pd.Series([row["valid_from"], row["last_seen"]]))
            xs += [start, end, None]
            ys += [row["price_usd"], row["price_usd"], None]
        trace_x = pd.Series(xs)
        if cutoff is not None:
            ys = [y if (x is None or x >= cutoff) else None for x, y in zip(xs, ys, strict=True)]
        fig.add_trace(
            go.Scatter(
                x=trace_x,
                y=ys,
                mode="lines",
                name=str(store),
                legendgroup="stores",
                line={"color": MUTED, "width": 1},
                opacity=0.6,
                connectgaps=False,
                hovertemplate=f"{store}: U$S %{{y:,.0f}}<extra></extra>",
                showlegend=False,
            )
        )
    if not daily.empty:
        d = daily.copy()
        d["date"] = pd.to_datetime(d["date"])
        if cutoff is not None:
            d = d[d["date"] >= cutoff]
        fig.add_trace(
            go.Scatter(
                x=d["date"],
                y=d["min_usd"],
                mode="lines",
                name="Precio más bajo",
                line={"color": SERIES_1, "width": 2, "shape": "hv"},
                hovertemplate="Más bajo: U$S %{y:,.0f}<extra></extra>",
            )
        )
    fig.add_trace(  # legend entry for the muted store lines
        go.Scatter(
            x=[None], y=[None], mode="lines", name="Cada tienda", line={"color": MUTED, "width": 1}
        )
    )
    return _layout(fig)


def family_chart(daily: pd.DataFrame, days: int | None) -> go.Figure:
    d = daily.copy()
    d["date"] = pd.to_datetime(d["date"])
    if days:
        d = d[d["date"] >= pd.Timestamp.now() - pd.Timedelta(days=days)]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["min_usd"],
            name="Más barato",
            mode="lines",
            line={"color": SERIES_1, "width": 2, "shape": "hv"},
            hovertemplate="Más barato: U$S %{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["median_usd"],
            name="Mediana de modelos",
            mode="lines",
            line={"color": SERIES_2, "width": 2, "shape": "hv"},
            hovertemplate="Mediana: U$S %{y:,.0f}<extra></extra>",
        )
    )
    return _layout(fig, height=340)


def cost_waterfall(labels: list[str], amounts: list[float], local_usd: float | None) -> go.Figure:
    """Landed cost line by line (horizontal waterfall) against the best local price (USD)."""
    texts = [money(amount, cur="USD") for amount in amounts]
    fig = go.Figure(
        go.Waterfall(
            orientation="h",
            y=[*labels, "Total puesto en Uruguay"],
            x=[*amounts, 0],
            measure=["relative"] * len(amounts) + ["total"],
            text=[*texts, money(sum(amounts), cur="USD")],
            textinfo="text",
            textposition="outside",
            increasing={"marker": {"color": SERIES_1}},
            decreasing={"marker": {"color": SERIES_1}},
            totals={"marker": {"color": SERIES_2}},
            connector={"line": {"color": MUTED, "width": 1}},
            hovertemplate="%{y}: U$S %{x:,.2f}<extra></extra>",
        )
    )
    if local_usd:
        fig.add_vline(
            x=local_usd,
            line_dash="dot",
            line_color=MUTED,
            annotation_text="Mejor precio en Uruguay",
            annotation_position="top",
        )
    fig.update_layout(
        height=110 + 38 * (len(labels) + 1),
        margin={"l": 8, "r": 70, "t": 40, "b": 30},
        showlegend=False,
        separators=",.",
        xaxis={"title": None, "tickprefix": "U$S ", "gridcolor": GRID, "zeroline": False},
        yaxis={"title": None, "autorange": "reversed"},
    )
    return fig


RANGES = {"30 días": 30, "90 días": 90, "180 días": 180, "Todo": None}
