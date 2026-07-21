"""
Feature engineering pipeline for hardware-pulse.

Responsibilities:
- Read resolved price data from price_snapshots
- Compute weekly median prices per canonical SKU
- Compute lag features and rolling median (time-series features)
- Compute price dispersion across sources per week/SKU
- Fetch weekly USD/UYU exchange rate
- Persist results to feature_snapshots table

Does NOT:
- Scrape or ingest raw listings (see pipelines/ingest.py)
- Resolve product identities (see pipelines/resolve.py)
- Train or evaluate models

Execution order:
    ingest → resolve → features
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.pipelines.fx import fetch_fx_rates

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROLLING_WINDOW = 4

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class FeatureResult:
    """Summary of a single feature engineering run."""

    run_at: str
    weeks_processed: int
    skus_processed: int
    rows_written: int
    fx_rates_fetched: int
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


# ---------------------------------------------------------------------------
# Core computations
# ---------------------------------------------------------------------------


def _safe_float(val: Any) -> float | None:
    """Convert to float or None if null/NaN."""
    if val is None:
        return None
    try:
        v = float(val)
        return None if math.isnan(v) else v
    except (ValueError, TypeError):
        return None


def _load_price_snapshots(
    conn: sqlite3.Connection,
    since: datetime | None,
) -> pd.DataFrame:
    """
    Load price snapshots from the database.

    Args:
        conn:
            Open SQLite connection.

        since:
            Optional lower bound timestamp.

    Returns:
        DataFrame with:
            - timestamp
            - canonical_product_id
            - price_usd
            - source
    """
    query = """
        SELECT
            timestamp,
            canonical_product_id,
            price_usd,
            source
        FROM price_snapshots
    """

    params: list[Any] = []

    if since is not None:
        query += " WHERE timestamp >= ?"
        params = [since.isoformat()]

    df = pd.read_sql_query(query, conn, params=params)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    return df


def _compute_weekly_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute weekly price features.

    Steps:
    1. Derive week_start (Monday UTC)
    2. Aggregate median/std price per SKU/week
    3. Compute lag features
    4. Compute rolling median

    Args:
        df:
            Raw price snapshot dataframe.

    Returns:
        Weekly feature dataframe.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    # -----------------------------------------------------------------------
    # Week normalization (Monday 00:00 UTC)
    # -----------------------------------------------------------------------

    df["week_start"] = df["timestamp"].dt.normalize() - pd.to_timedelta(
        df["timestamp"].dt.weekday, unit="D"
    )

    # -----------------------------------------------------------------------
    # Weekly aggregation
    # -----------------------------------------------------------------------

    weekly = (
        df.groupby(["week_start", "canonical_product_id"])["price_usd"]
        .agg(
            mediana_semanal="median",
            dispersion_precios="std",
        )
        .reset_index()
    )

    # -----------------------------------------------------------------------
    # Sort for time-series operations
    # -----------------------------------------------------------------------

    weekly = weekly.sort_values(["canonical_product_id", "week_start"])

    # -----------------------------------------------------------------------
    # Fill missing calendar weeks so shift() is date-aware
    # -----------------------------------------------------------------------

    all_weeks = pd.date_range(
        weekly["week_start"].min(),
        weekly["week_start"].max(),
        freq="W-MON",
    )

    def _fill_missing_weeks(group: pd.DataFrame, sku: str) -> pd.DataFrame:
        group = group.set_index("week_start").reindex(all_weeks)
        group["canonical_product_id"] = sku
        return group.reset_index().rename(columns={"index": "week_start"})

    weekly = (
        weekly.groupby("canonical_product_id", group_keys=False)
        .apply(lambda g: _fill_missing_weeks(g, g.name))
        .reset_index(drop=True)
    )

    weekly = weekly.sort_values(["canonical_product_id", "week_start"])

    # -----------------------------------------------------------------------
    # Lag features
    # -----------------------------------------------------------------------

    weekly["precio_lag_1"] = weekly.groupby("canonical_product_id")["mediana_semanal"].shift(1)

    weekly["precio_lag_2"] = weekly.groupby("canonical_product_id")["mediana_semanal"].shift(2)

    # -----------------------------------------------------------------------
    # Rolling median
    # -----------------------------------------------------------------------

    weekly["mediana_movil"] = weekly.groupby("canonical_product_id")["mediana_semanal"].transform(
        lambda s: s.rolling(
            window=ROLLING_WINDOW,
            min_periods=1,
        ).median()
    )

    # -----------------------------------------------------------------------
    # SQLite-friendly formatting
    # -----------------------------------------------------------------------

    weekly["week_start"] = weekly["week_start"].dt.strftime("%Y-%m-%d")

    return weekly.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def build_features(
    conn: sqlite3.Connection,
    since: datetime | None = None,
    run_at: datetime | None = None,
) -> FeatureResult:
    """
    Run the feature engineering pipeline.

    Args:
        conn:
            Open SQLite connection.

        since:
            Optional lower timestamp bound.

        run_at:
            Pipeline execution timestamp.

    Returns:
        FeatureResult
    """
    if run_at is None:
        run_at = datetime.now(UTC)

    run_at_str = run_at.isoformat()

    errors: list[str] = []

    # -----------------------------------------------------------------------
    # Load data
    # -----------------------------------------------------------------------

    logger.info(
        "Loading price snapshots (since=%s)",
        since,
    )

    df = _load_price_snapshots(conn, since)

    if df.empty:
        logger.warning("No price snapshots found — nothing to compute")

        return FeatureResult(
            run_at=run_at_str,
            weeks_processed=0,
            skus_processed=0,
            rows_written=0,
            fx_rates_fetched=0,
        )

    # -----------------------------------------------------------------------
    # Compute features
    # -----------------------------------------------------------------------

    logger.info(
        "Computing weekly features for %d snapshots",
        len(df),
    )

    weekly = _compute_weekly_features(df)

    weeks_processed: int = int(weekly["week_start"].nunique())  # type: ignore[arg-type]

    skus_processed: int = int(weekly["canonical_product_id"].nunique())  # type: ignore[arg-type]

    logger.info(
        "Computed features: %d weeks × %d SKUs = %d rows",
        weeks_processed,
        skus_processed,
        len(weekly),
    )

    # -----------------------------------------------------------------------
    # Fetch FX rates
    # -----------------------------------------------------------------------

    unique_weeks = weekly["week_start"].tolist()

    logger.info(
        "Fetching FX rates for %d unique weeks",
        len(set(unique_weeks)),
    )

    fx_rates = fetch_fx_rates(unique_weeks)

    fx_rates_fetched = sum(1 for v in fx_rates.values() if v is not None)

    weekly["usd_uyu_rate"] = weekly["week_start"].map(fx_rates)  # type: ignore

    # -----------------------------------------------------------------------
    # Persist
    # -----------------------------------------------------------------------

    rows_written = 0

    rows_to_insert = []
    for _, row in weekly.iterrows():
        try:
            rows_to_insert.append(
                {
                    "week_start": row["week_start"],
                    "canonical_product_id": row["canonical_product_id"],
                    "run_at": run_at_str,
                    "precio_lag_1": _safe_float(row["precio_lag_1"]),
                    "precio_lag_2": _safe_float(row["precio_lag_2"]),
                    "mediana_movil": _safe_float(row["mediana_movil"]),
                    "dispersion_precios": _safe_float(row["dispersion_precios"]),
                    "usd_uyu_rate": _safe_float(row["usd_uyu_rate"]),
                }
            )
        except Exception as exc:
            msg = (
                f"Failed to build feature row "
                f"({row['week_start']}, "
                f"{row['canonical_product_id']}): {exc}"
            )
            logger.error(msg)
            errors.append(msg)

    if rows_to_insert:
        with conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO feature_snapshots (
                    week_start,
                    canonical_product_id,
                    run_at,
                    precio_lag_1,
                    precio_lag_2,
                    mediana_movil,
                    dispersion_precios,
                    usd_uyu_rate
                )
                VALUES (
                    :week_start,
                    :canonical_product_id,
                    :run_at,
                    :precio_lag_1,
                    :precio_lag_2,
                    :mediana_movil,
                    :dispersion_precios,
                    :usd_uyu_rate
                )
                """,
                rows_to_insert,
            )
        rows_written = len(rows_to_insert)

    # -----------------------------------------------------------------------
    # Final summary
    # -----------------------------------------------------------------------

    logger.info(
        "Feature pipeline complete: %d rows written",
        rows_written,
    )

    return FeatureResult(
        run_at=run_at_str,
        weeks_processed=weeks_processed,
        skus_processed=skus_processed,
        rows_written=rows_written,
        fx_rates_fetched=fx_rates_fetched,
        errors=errors,
    )
