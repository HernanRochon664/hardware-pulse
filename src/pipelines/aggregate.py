"""
Aggregate stage: derived tables used by the dashboard and the ML signal.

- ``quality_issues``: current offers whose price is implausible for their
  product (less than half / more than double the median of the other stores)
  or below a category floor. They are kept but excluded from every aggregate.
- ``product_daily``: lowest valid price of each product per local day,
  reconstructed from the price periods (history charts, sparklines, ML).
- ``family_daily``: min / p25 / median of product prices per family and day.
- ``product_stats``: current best offer, 180-day range, 7/30-day change,
  last price change and historic-low flag per product.

A *current offer* is a listing that is active, new (not open box / used), not
out of stock, was seen in the last successful scrape of its store and
category, and was seen less than 72 hours ago.
"""

from __future__ import annotations

import logging
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from src.settings import LOCAL_TZ, local_date, to_iso, utcnow

logger = logging.getLogger(__name__)

FRESH_HOURS = 72
OUTLIER_LOW, OUTLIER_HIGH = 0.5, 2.0
CATEGORY_FLOOR_USD = {"gpu": 20.0, "cpu": 2.0, "ram": 3.0, "ssd": 10.0}
HISTORIC_LOW_MIN_DAYS = 14

CURRENT_OFFERS_SQL = """
WITH last_ok AS (
    SELECT store_id, category, MAX(started_at) AS started_at
    FROM scrape_runs WHERE status = 'ok' GROUP BY store_id, category
),
latest AS (
    SELECT p.* FROM price_periods p
    WHERE p.id = (
        SELECT p2.id FROM price_periods p2 WHERE p2.listing_id = p.listing_id
        ORDER BY p2.valid_from DESC, p2.id DESC LIMIT 1
    )
)
SELECT l.id AS listing_id, l.product_id, l.store_id, l.category, l.url, l.title,
       l.last_seen_at, latest.price, latest.regular_price, latest.currency,
       latest.price_usd, latest.in_stock, latest.valid_from
FROM listings l
JOIN latest ON latest.listing_id = l.id
LEFT JOIN last_ok o ON o.store_id = l.store_id AND o.category = l.category
WHERE l.is_active = 1
  AND l.product_id IS NOT NULL
  AND l.condition = 'new'
  AND (latest.in_stock IS NULL OR latest.in_stock = 1)
  AND l.last_seen_at >= :fresh_cutoff
  AND (o.started_at IS NULL OR l.last_seen_at >= o.started_at)
"""


@dataclass
class AggregateStats:
    current_offers: int = 0
    quality_issues: int = 0
    product_days: int = 0
    family_days: int = 0
    products_with_offers: int = 0


def current_offers(
    conn: sqlite3.Connection, now: datetime | None = None, *, include_flagged: bool = False
) -> pd.DataFrame:
    now = now or utcnow()
    cutoff = to_iso(now - timedelta(hours=FRESH_HOURS))
    sql = CURRENT_OFFERS_SQL
    if not include_flagged:
        sql += " AND l.id NOT IN (SELECT listing_id FROM quality_issues)"
    return pd.read_sql_query(sql, conn, params={"fresh_cutoff": cutoff})


def aggregate(conn: sqlite3.Connection, now: datetime | None = None) -> AggregateStats:
    now = now or utcnow()
    stats = AggregateStats()
    with conn:
        stats.quality_issues = _rebuild_quality_issues(conn, now)
        offers = current_offers(conn, now)
        stats.current_offers = len(offers)
        daily = _rebuild_product_daily(conn)
        stats.product_days = len(daily)
        stats.family_days = _rebuild_family_daily(conn, daily)
        stats.products_with_offers = _rebuild_product_stats(conn, offers, daily, now)
    logger.info("Aggregated %s", stats)
    return stats


def _rebuild_quality_issues(conn: sqlite3.Connection, now: datetime) -> int:
    conn.execute("DELETE FROM quality_issues")
    offers = current_offers(conn, now, include_flagged=True).to_dict("records")
    issues: list[tuple[int, str, str]] = []
    by_product: dict[int, list[tuple[int, float]]] = {}
    for offer in offers:
        listing_id, price = int(offer["listing_id"]), float(offer["price_usd"])
        if price < CATEGORY_FLOOR_USD.get(str(offer["category"]), 0.0):
            issues.append((listing_id, "below_floor", f"U$S {price:.2f}"))
        else:
            by_product.setdefault(int(offer["product_id"]), []).append((listing_id, price))
    for group in by_product.values():
        if len(group) < 3:
            continue
        median = statistics.median(price for _, price in group)
        for listing_id, price in group:
            ratio = price / median if median else 1.0
            if ratio < OUTLIER_LOW or ratio > OUTLIER_HIGH:
                kind = "too_cheap" if ratio < OUTLIER_LOW else "too_expensive"
                issues.append((listing_id, kind, f"{ratio:.2f}x la mediana {median:.0f}"))
    detected = to_iso(now)
    conn.executemany(
        "INSERT INTO quality_issues (listing_id, kind, detail, detected_at) VALUES (?, ?, ?, ?)",
        [(lid, kind, detail, detected) for lid, kind, detail in issues],
    )
    return len(issues)


def _local_dates(series: pd.Series) -> pd.Series:
    stamps = pd.to_datetime(series, utc=True).dt.tz_convert(LOCAL_TZ)
    return stamps.dt.normalize().dt.tz_localize(None)


def _rebuild_product_daily(conn: sqlite3.Connection) -> pd.DataFrame:
    periods = pd.read_sql_query(
        """
        SELECT p.listing_id, l.product_id, l.store_id, p.price_usd, p.valid_from, p.last_seen
        FROM price_periods p JOIN listings l ON l.id = p.listing_id
        WHERE l.product_id IS NOT NULL AND l.condition = 'new'
          AND (p.in_stock IS NULL OR p.in_stock = 1)
          AND l.id NOT IN (SELECT listing_id FROM quality_issues)
        """,
        conn,
    )
    conn.execute("DELETE FROM product_daily")
    if periods.empty:
        return pd.DataFrame(columns=["product_id", "date", "min_usd", "n_offers", "n_stores"])
    start = _local_dates(pd.Series(periods["valid_from"]))
    end = _local_dates(pd.Series(periods["last_seen"]))
    days = ((end - start).dt.days + 1).clip(lower=1).to_numpy()
    expanded = periods.loc[periods.index.repeat(days)].copy()
    offsets = np.concatenate([np.arange(n) for n in days])
    expanded["date"] = np.repeat(start.to_numpy(), days) + pd.to_timedelta(offsets, unit="D")
    daily = (
        expanded.groupby(["product_id", "date"])
        .agg(
            min_usd=("price_usd", "min"),
            n_offers=("listing_id", "nunique"),
            n_stores=("store_id", "nunique"),
        )
        .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y-%m-%d")
    conn.executemany(
        "INSERT INTO product_daily (product_id, date, min_usd, n_offers, n_stores) "
        "VALUES (?, ?, ?, ?, ?)",
        daily[["product_id", "date", "min_usd", "n_offers", "n_stores"]].itertuples(
            index=False, name=None
        ),
    )
    return daily


def _rebuild_family_daily(conn: sqlite3.Connection, daily: pd.DataFrame) -> int:
    conn.execute("DELETE FROM family_daily")
    if daily.empty:
        return 0
    families = pd.read_sql_query(
        "SELECT id AS product_id, family_id FROM products WHERE family_id IS NOT NULL", conn
    )
    merged = daily.merge(families, on="product_id")
    if merged.empty:
        return 0
    grouped = (
        merged.groupby(["family_id", "date"])["min_usd"]
        .agg(
            min_usd="min",
            p25_usd=lambda s: float(s.quantile(0.25)),
            median_usd="median",
            n_products="count",
        )
        .reset_index()
    )
    conn.executemany(
        "INSERT INTO family_daily (family_id, date, min_usd, p25_usd, median_usd, n_products) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        grouped[["family_id", "date", "min_usd", "p25_usd", "median_usd", "n_products"]]
        .round(2)
        .itertuples(index=False, name=None),
    )
    return len(grouped)


@dataclass
class _BestOffer:
    price_usd: float
    listing_id: int
    store_id: str
    regular_usd: float | None
    n_offers: int
    n_stores: int


def _value_on_or_before(
    history: list[tuple[str, float]], day: date, tolerance_days: int = 3
) -> float | None:
    """Price of a (date, price) history on ``day`` or the closest earlier day."""
    latest, earliest = day.isoformat(), (day - timedelta(days=tolerance_days)).isoformat()
    candidates = [price for d, price in history if earliest <= d <= latest]
    return candidates[-1] if candidates else None


def _best_offers(offers: pd.DataFrame) -> dict[int, _BestOffer]:
    grouped: dict[int, list[dict]] = {}
    for offer in offers.to_dict("records"):
        grouped.setdefault(int(offer["product_id"]), []).append(offer)
    best: dict[int, _BestOffer] = {}
    for pid, group in grouped.items():
        top = min(group, key=lambda o: (float(o["price_usd"]), int(o["listing_id"])))
        regular = top["regular_price"]
        regular_usd = None
        if regular is not None and not pd.isna(regular) and float(top["price"]):
            regular_usd = round(float(top["price_usd"]) * float(regular) / float(top["price"]), 2)
        best[pid] = _BestOffer(
            price_usd=float(top["price_usd"]),
            listing_id=int(top["listing_id"]),
            store_id=str(top["store_id"]),
            regular_usd=regular_usd,
            n_offers=len(group),
            n_stores=len({str(o["store_id"]) for o in group}),
        )
    return best


def _rebuild_product_stats(
    conn: sqlite3.Connection, offers: pd.DataFrame, daily: pd.DataFrame, now: datetime
) -> int:
    conn.execute("DELETE FROM product_stats")
    today = local_date(now)
    updated = to_iso(now)
    history: dict[int, list[tuple[str, float]]] = {}
    for record in daily.sort_values("date").to_dict("records"):
        history.setdefault(int(record["product_id"]), []).append(
            (str(record["date"]), float(record["min_usd"]))
        )
    best = _best_offers(offers)
    since_180 = (today - timedelta(days=180)).isoformat()

    rows = []
    for pid in set(history) | set(best):
        series = history.get(pid, [])
        prices = [price for _, price in series]
        recent = [price for d, price in series if d >= since_180]
        offer = best.get(pid)
        changes: dict[int, float | None] = {7: None, 30: None}
        if offer is not None:
            for days in changes:
                past = _value_on_or_before(series, today - timedelta(days=days))
                changes[days] = round((offer.price_usd - past) / past, 4) if past else None
        last_change = None
        for (_, previous), (day, price) in zip(series, series[1:], strict=False):
            if round(price, 2) != round(previous, 2):
                last_change = day
        # A flat price is not a "historic low": the product must have been at
        # least 2% more expensive at some point of its history.
        is_low = bool(
            offer is not None
            and len(series) >= HISTORIC_LOW_MIN_DAYS
            and offer.price_usd <= min(prices) + 0.005
            and max(prices) >= offer.price_usd * 1.02
        )
        rows.append(
            (
                pid,
                offer.price_usd if offer else None,
                offer.listing_id if offer else None,
                offer.store_id if offer else None,
                offer.regular_usd if offer else None,
                offer.n_offers if offer else 0,
                offer.n_stores if offer else 0,
                min(recent) if recent else None,
                max(recent) if recent else None,
                changes[7],
                changes[30],
                last_change,
                int(is_low),
                len(series),
                updated,
            )
        )
    conn.executemany(
        """
        INSERT INTO product_stats (
            product_id, best_price_usd, best_listing_id, best_store_id, best_regular_usd,
            n_offers, n_stores, min_180d, max_180d, change_7d, change_30d, last_change_at,
            is_historic_low, history_days, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(best)
