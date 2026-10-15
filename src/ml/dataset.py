"""
Daily product panel, point-in-time features and the buy/wait label.

The panel starts from ``product_daily`` (lowest valid price per product and
local day, built by the aggregate stage). Each product is reindexed to a
continuous calendar so that "7 days ago" really means seven calendar days;
days without a valid offer stay empty.

Every feature at day ``t`` uses data up to and including ``t`` only (trailing
windows and backward shifts). The label looks at ``(t, t + horizon]``: it is 1
when the lowest price in that window is at least ``drop`` below the price at
``t``, i.e. waiting would have paid off.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.domain.models import Category

CATEGORIES = [c.value for c in Category]
FEATURES = [
    "ret_1",
    "ret_7",
    "ret_14",
    "ret_30",
    "dist_min_30",
    "dist_med_30",
    "dist_max_30",
    "dist_min_90",
    "dist_med_90",
    "dist_max_90",
    "vol_30",
    "days_since_change",
    "n_offers",
    "n_stores",
    "family_pos",
    "family_mom_7",
    "age_days",
    "near_black_friday",
    *[f"cat_{c}" for c in CATEGORIES],
]
FFILL_LIMIT = 3  # a store missing for up to 3 days does not break the lookbacks
# Bounds keep a single glitch (a v1 parsing error, a placeholder price) from
# dominating the linear model; they cut overconfident tails in the backtest.
FEATURE_CLIP: dict[str, tuple[float, float]] = {
    **{f: (-0.5, 0.5) for f in FEATURES if f.startswith(("ret_", "dist_", "family_mom"))},
    "family_pos": (-0.7, 2.0),
    "vol_30": (0.0, 0.3),
}


@dataclass(frozen=True)
class LabelSpec:
    horizon_days: int = 14
    drop: float = 0.05


def load_panel(conn: sqlite3.Connection) -> pd.DataFrame:
    """product_daily joined with category, family and the family median."""
    daily = pd.read_sql_query(
        """
        SELECT d.product_id, d.date, d.min_usd AS price, d.n_offers, d.n_stores,
               p.category, p.family_id, f.median_usd AS family_median
        FROM product_daily d
        JOIN products p ON p.id = d.product_id
        LEFT JOIN family_daily f ON f.family_id = p.family_id AND f.date = d.date
        """,
        conn,
    )
    daily["date"] = pd.to_datetime(daily["date"])
    return daily


def black_friday(year: int) -> date:
    """Day after the fourth Thursday of November."""
    first = date(year, 11, 1)
    thursday = first + timedelta(days=(3 - first.weekday()) % 7 + 21)
    return thursday + timedelta(days=1)


def _near_black_friday(dates: pd.Series, window: int = 21) -> np.ndarray:
    out = np.zeros(len(dates))
    for i, d in enumerate(dates.dt.date):
        bf = black_friday(d.year)
        out[i] = 1.0 if 0 <= (bf - d).days <= window or 0 <= (d - bf).days <= 3 else 0.0
    return out


def _product_features(group: pd.DataFrame, spec: LabelSpec) -> pd.DataFrame:
    g = group.set_index("date").sort_index()
    calendar = pd.date_range(g.index.min(), g.index.max(), freq="D")
    g = g.reindex(calendar)
    g.index.name = "date"
    price = pd.Series(g["price"], dtype=float)
    past = price.ffill(limit=FFILL_LIMIT)

    out = pd.DataFrame(index=g.index)
    out["price"] = price
    for k in (1, 7, 14, 30):
        out[f"ret_{k}"] = price / past.shift(k) - 1
    for window, min_periods in ((30, 7), (90, 21)):
        roll = price.rolling(window, min_periods=min_periods)
        out[f"dist_min_{window}"] = price / roll.min() - 1
        out[f"dist_med_{window}"] = price / roll.median() - 1
        out[f"dist_max_{window}"] = price / roll.max() - 1
    out["vol_30"] = past.pct_change(fill_method=None).rolling(30, min_periods=7).std()
    changed = past.ne(past.shift()) & past.notna() & past.shift().notna()
    run_id = changed.cumsum()
    out["days_since_change"] = np.log1p(run_id.groupby(run_id).cumcount())
    out["n_offers"] = g["n_offers"]
    out["n_stores"] = g["n_stores"]
    family_median = pd.Series(g["family_median"], dtype=float)
    out["family_pos"] = price / family_median - 1
    out["family_mom_7"] = family_median / family_median.ffill(limit=FFILL_LIMIT).shift(7) - 1
    out["age_days"] = np.log1p(np.arange(len(g)))
    out["history_days"] = price.notna().cumsum()

    # label: lowest observed price in (t, t + h] versus the price at t
    future_min = (
        price.iloc[::-1].rolling(spec.horizon_days, min_periods=1).min().iloc[::-1].shift(-1)
    )
    end_price = past.shift(-spec.horizon_days)
    out["future_min"] = future_min
    out["end_price"] = end_price
    out["label"] = (future_min <= price * (1 - spec.drop)).astype(float)
    out.loc[future_min.isna(), "label"] = np.nan
    # the window must be complete: the product must be tracked until t + h
    out.loc[out.index > calendar[-1] - pd.Timedelta(days=spec.horizon_days), "label"] = np.nan
    out["product_id"] = group["product_id"].iloc[0]
    out["category"] = group["category"].iloc[0]
    return out.reset_index()


def build_dataset(panel: pd.DataFrame, spec: LabelSpec | None = None) -> pd.DataFrame:
    """One row per product and day with a valid price: features, label, context."""
    spec = spec or LabelSpec()
    if panel.empty:
        return pd.DataFrame(columns=["product_id", "date", *FEATURES, "label"])
    parts = [_product_features(group, spec) for _, group in panel.groupby("product_id")]
    data = pd.concat(parts, ignore_index=True)
    data = data.loc[data["price"].notna()].copy()
    data["near_black_friday"] = _near_black_friday(pd.Series(data["date"]))
    for c in CATEGORIES:
        data[f"cat_{c}"] = (data["category"] == c).astype(float)
    data[FEATURES] = data[FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    for feature, (low, high) in FEATURE_CLIP.items():
        data[feature] = data[feature].clip(low, high)
    return data.reset_index(drop=True)
