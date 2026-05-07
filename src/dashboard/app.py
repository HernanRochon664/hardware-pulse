"""
Streamlit dashboard for Hardware Pulse.

Responsibilities:
- Display market summary with deal signals
- Show product-specific price history and store prices
- Visualize price trends with Plotly charts

Does NOT:
- Define business logic (see signals.py)
- Access database directly (see queries.py)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

from src.dashboard.queries import get_connection, get_current_prices, get_market_summary, get_price_history
from src.dashboard.signals import detect_signal, format_signal

st.set_page_config(page_title="Hardware Pulse", layout="wide")
st.title("Hardware Pulse 💸")

db_path = Path("data/hardware_pulse.db")
if not db_path.exists():
    st.error("Database not found. Run ingestion pipeline first.")
    st.stop()

conn = get_connection(db_path)

tab_summary, tab_product = st.tabs(["📊 Resumen", "🔎 Producto"])

with tab_summary:
    st.header("Market Summary")
    summary = get_market_summary(conn)

    if not summary:
        st.warning("No data available.")
    else:
        for item in summary:
            item["signal"] = detect_signal(item["current_price"], item["median_price"])
            item["signal_info"] = format_signal(item["signal"], item["pct_diff"])

        display_df = [
            {
                "SKU": item["sku"],
                "Current Price": f"${item['current_price']:.2f}",
                "Median": f"${item['median_price']:.2f}",
                "vs Median": f"{item['pct_diff']:+.1f}%",
                "Signal": item["signal_info"]["emoji"],
                "Last Updated": item.get("latest_timestamp", "N/A"),
            }
            for item in summary
        ]
        st.dataframe(
            data=display_df,
            use_container_width=True,
            hide_index=True,
        )

        deals = [x for x in summary if x["signal"] == "deal"]
        if deals:
            st.subheader("🔥 Best Deals")
            for item in deals:
                st.markdown(
                    f"**{item['sku']}** — `${item['current_price']:.2f}` "
                    f"vs `${item['median_price']:.2f}` median ({item['pct_diff']:+.1f}%)"
                )

with tab_product:
    skus = [r["sku"] for r in get_market_summary(conn)]
    if not skus:
        st.warning("No SKUs available.")
    else:
        selected = st.selectbox("Select SKU", skus)

        if selected:
            history = get_price_history(conn, selected)
            current_prices = get_current_prices(conn, selected)

            if history:
                import pandas as pd
                import plotly.express as px

                df: pd.DataFrame = pd.DataFrame(history)  # type: ignore[assignment]
                df["timestamp"] = pd.to_datetime(df["timestamp"])  # type: ignore[arg-type]

                median: float = float(df["price_usd"].median())  # type: ignore[assignment]

                fig = px.line(
                    df,
                    x="timestamp",
                    y="price_usd",
                    color="source",
                    markers=True,
                    title=f"Price History: {selected}",
                )
                fig.add_hline(
                    y=median,
                    line_dash="dash",
                    line_color="green",
                    annotation_text=f"Median: ${median:.2f}",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No price history.")

            if current_prices:
                st.subheader("Current Prices by Store")
                for p in current_prices:
                    ts = p["timestamp"][:16]
                    st.markdown(f"**{p['source']}** ({p['seller']}): ${p['price_usd']:.2f} — {ts}")
            else:
                st.warning("No prices in the last 48 hours.")

            if history:
                current_min = min(p["price_usd"] for p in current_prices)
                signal = detect_signal(current_min, median)
                signal_info = format_signal(signal, ((current_min - median) / median) * 100)

                st.markdown(
                    f"## {signal_info['emoji']} **{signal_info['label']}** "
                    f"({signal_info['pct_diff']:+.1f}% vs median)"
                )
