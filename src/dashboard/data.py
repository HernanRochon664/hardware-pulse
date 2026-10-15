"""
Read-only data access for the dashboard.

The database is the local file when it exists (``HP_DB_PATH`` or
``data/hardware_pulse.db``); otherwise the copy published by the scrape
workflow is downloaded once per hour. Every query reads the derived tables
built by the aggregate stage, so pages stay fast.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from src.pipelines.status import build_status
from src.settings import db_path
from src.storage.db import connect
from src.storage.remote import download_database

CACHE_TTL = 600


@st.cache_resource(ttl=3600, show_spinner="Descargando los precios más recientes…")
def _database_file() -> str:
    local = db_path()
    if local.exists():
        return str(local)
    target = Path(tempfile.gettempdir()) / "hardware_pulse" / "hardware_pulse.db"
    return str(download_database(target))


def _conn() -> sqlite3.Connection:
    return connect(_database_file(), readonly=True)


def _query(sql: str, params: list | dict | None = None) -> pd.DataFrame:
    conn = _conn()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()


@st.cache_data(ttl=CACHE_TTL)
def status() -> dict:
    conn = _conn()
    try:
        return build_status(conn)
    finally:
        conn.close()


@st.cache_data(ttl=CACHE_TTL)
def usd_uyu() -> float | None:
    df = _query("SELECT usd_uyu FROM fx_rates ORDER BY date DESC LIMIT 1")
    return float(df.iloc[0, 0]) if not df.empty else None


@st.cache_data(ttl=CACHE_TTL)
def stores() -> pd.DataFrame:
    return _query("SELECT id, name, base_url FROM stores ORDER BY name")


@st.cache_data(ttl=CACHE_TTL)
def catalog() -> pd.DataFrame:
    """One row per product with a current offer (plus 30-day sparkline)."""
    df = _query(
        """
        SELECT p.id, p.name, p.category, p.brand, p.image_url, p.family_id,
               f.name AS family, s.best_price_usd, s.best_store_id, st.name AS best_store,
               s.best_regular_usd, s.n_offers, s.n_stores, s.min_180d, s.max_180d,
               s.change_7d, s.change_30d, s.last_change_at, s.is_historic_low, s.history_days,
               sig.recommendation, sig.p_drop
        FROM product_stats s
        JOIN products p ON p.id = s.product_id
        LEFT JOIN families f ON f.id = p.family_id
        LEFT JOIN stores st ON st.id = s.best_store_id
        LEFT JOIN signals sig ON sig.entity_type = 'product' AND sig.entity_id = CAST(p.id AS TEXT)
        WHERE s.best_price_usd IS NOT NULL
        """
    )
    spark = _query(
        "SELECT product_id, date, min_usd FROM product_daily "
        "WHERE date >= date('now', '-30 days') ORDER BY product_id, date"
    )
    series = spark.groupby("product_id")["min_usd"].apply(list).to_dict()
    df["sparkline"] = [series.get(i, []) for i in df["id"].tolist()]
    return df


@st.cache_data(ttl=CACHE_TTL)
def product(product_id: int) -> dict | None:
    df = _query(
        """
        SELECT p.*, f.name AS family, s.best_price_usd, s.best_store_id, s.best_regular_usd,
               s.n_offers, s.n_stores, s.min_180d, s.max_180d, s.change_7d, s.change_30d,
               s.last_change_at, s.is_historic_low, s.history_days
        FROM products p
        LEFT JOIN families f ON f.id = p.family_id
        LEFT JOIN product_stats s ON s.product_id = p.id
        WHERE p.id = ?
        """,
        [product_id],
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    row["specs"] = json.loads(row.get("specs_json") or "{}")
    return row


@st.cache_data(ttl=CACHE_TTL)
def offers(product_id: int) -> pd.DataFrame:
    """Every listing of a product with its latest price and whether it is a current offer."""
    from src.pipelines.aggregate import FRESH_HOURS

    return _query(
        f"""
        WITH latest AS (
            SELECT p.* FROM price_periods p
            WHERE p.id = (SELECT p2.id FROM price_periods p2 WHERE p2.listing_id = p.listing_id
                          ORDER BY p2.valid_from DESC, p2.id DESC LIMIT 1)
        ),
        last_ok AS (
            SELECT store_id, category, MAX(started_at) AS started_at FROM scrape_runs
            WHERE status = 'ok' GROUP BY store_id, category
        )
        SELECT l.id AS listing_id, l.store_id, st.name AS store, l.url, l.title, l.condition,
               l.is_active, l.last_seen_at, latest.price, latest.regular_price, latest.currency,
               latest.price_usd, latest.in_stock, latest.valid_from,
               q.kind AS quality_issue,
               (l.is_active = 1 AND l.condition = 'new'
                AND (latest.in_stock IS NULL OR latest.in_stock = 1)
                AND l.last_seen_at >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-{FRESH_HOURS} hours')
                AND (o.started_at IS NULL OR l.last_seen_at >= o.started_at)
                AND q.kind IS NULL) AS is_current
        FROM listings l
        JOIN latest ON latest.listing_id = l.id
        JOIN stores st ON st.id = l.store_id
        LEFT JOIN last_ok o ON o.store_id = l.store_id AND o.category = l.category
        LEFT JOIN quality_issues q ON q.listing_id = l.id
        WHERE l.product_id = ?
        ORDER BY is_current DESC, latest.price_usd
        """,
        [product_id],
    )


@st.cache_data(ttl=CACHE_TTL)
def price_periods(product_id: int) -> pd.DataFrame:
    return _query(
        """
        SELECT st.name AS store, l.store_id, p.price_usd, p.in_stock, p.valid_from, p.last_seen,
               p.origin, l.condition
        FROM price_periods p
        JOIN listings l ON l.id = p.listing_id
        JOIN stores st ON st.id = l.store_id
        WHERE l.product_id = ? ORDER BY p.valid_from
        """,
        [product_id],
    )


@st.cache_data(ttl=CACHE_TTL)
def product_daily(product_id: int) -> pd.DataFrame:
    return _query(
        "SELECT date, min_usd, n_stores FROM product_daily WHERE product_id = ? ORDER BY date",
        [product_id],
    )


@st.cache_data(ttl=CACHE_TTL)
def family_products(family_id: str) -> pd.DataFrame:
    return catalog().query("family_id == @family_id").sort_values("best_price_usd")


@st.cache_data(ttl=CACHE_TTL)
def families() -> pd.DataFrame:
    df = _query(
        """
        SELECT f.id, f.name, f.category, f.specs_json,
               MIN(s.best_price_usd) AS min_usd,
               COUNT(*) AS n_products, SUM(s.n_offers) AS n_offers
        FROM families f
        JOIN products p ON p.family_id = f.id
        JOIN product_stats s ON s.product_id = p.id
        WHERE s.best_price_usd IS NOT NULL
        GROUP BY f.id
        """
    )
    daily = _query(
        "SELECT family_id, date, min_usd, median_usd FROM family_daily "
        "WHERE date >= date('now', '-90 days') ORDER BY family_id, date"
    )
    spark = daily.groupby("family_id")["min_usd"].apply(list).to_dict()
    df["sparkline"] = [spark.get(i, []) for i in df["id"].tolist()]

    def change_30d(values: list) -> float | None:
        if len(values) < 2 or not values[0]:
            return None
        window = values[-31:]
        return (window[-1] - window[0]) / window[0]

    df["change_30d"] = df["sparkline"].apply(change_30d).astype(float)
    df["specs"] = df["specs_json"].apply(lambda s: json.loads(s or "{}"))
    return df


@st.cache_data(ttl=CACHE_TTL)
def family_daily(family_id: str) -> pd.DataFrame:
    return _query(
        "SELECT date, min_usd, p25_usd, median_usd, n_products FROM family_daily "
        "WHERE family_id = ? ORDER BY date",
        [family_id],
    )


@st.cache_data(ttl=CACHE_TTL)
def signal(product_id: int) -> dict | None:
    df = _query(
        "SELECT * FROM signals WHERE entity_type = 'product' AND entity_id = ?",
        [str(product_id)],
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    row["details"] = json.loads(row.get("details_json") or "{}")
    return row


@st.cache_data(ttl=CACHE_TTL)
def active_model() -> dict | None:
    df = _query("SELECT * FROM model_registry WHERE is_active = 1 ORDER BY trained_at DESC LIMIT 1")
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    row["params"] = json.loads(row["params_json"])
    row["metrics"] = json.loads(row["metrics_json"])
    return row


@st.cache_data(ttl=CACHE_TTL)
def unresolved() -> pd.DataFrame:
    return _query(
        """
        SELECT st.name AS store, l.category, l.title, l.match_status, l.match_notes, l.url
        FROM listings l JOIN stores st ON st.id = l.store_id
        WHERE l.is_active = 1 AND l.match_status = 'unparsed'
        ORDER BY l.category, l.title
        """
    )


@st.cache_data(ttl=CACHE_TTL)
def quality_issues() -> pd.DataFrame:
    return _query(
        """
        SELECT st.name AS store, l.title, q.kind, q.detail, l.url
        FROM quality_issues q JOIN listings l ON l.id = q.listing_id
        JOIN stores st ON st.id = l.store_id ORDER BY q.kind
        """
    )
