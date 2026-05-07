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
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FX_API_URL = (
    "https://cdn.jsdelivr.net/npm/"
    "@fawazahmed0/currency-api@{date}/"
    "v1/currencies/usd.json"
)

ROLLING_WINDOW = 4
REQUEST_TIMEOUT = 10

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
# Exchange rate fetching
# ---------------------------------------------------------------------------


def _fetch_fx_rates(week_starts: list[str]) -> dict[str, float | None]:
    """
    Fetch USD/UYU FX rates for all requested dates.

    Args:
        week_starts:
            List of ISO dates (YYYY-MM-DD)

    Returns:
        Dict mapping date -> USD/UYU rate
    """
    results: dict[str, float | None] = {}

    unique_dates = sorted(set(week_starts))

    for date_str in unique_dates:
        url = FX_API_URL.format(date=date_str)

        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()

            usd_rates = data.get("usd", {})
            rate = usd_rates.get("uyu")

            if rate is None:
                logger.warning(
                    "No USD/UYU FX rate found for %s",
                    date_str,
                )
                results[date_str] = None
                continue

            results[date_str] = float(rate)

            logger.debug(
                "Fetched USD/UYU FX rate | date=%s | rate=%.4f",
                date_str,
                results[date_str],
            )

        except Exception as exc:
            logger.warning(
                "Failed to fetch FX rate for %s: %s",
                date_str,
                exc,
            )
            results[date_str] = None

    return results


# ---------------------------------------------------------------------------
# Core computations
# ---------------------------------------------------------------------------


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

    params: tuple[Any, ...] = ()

    if since is not None:
        query += " WHERE timestamp >= ?"
        params = (since.isoformat(),)

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
    # Lag features
    # -----------------------------------------------------------------------

    weekly["precio_lag_1"] = weekly.groupby("canonical_product_id")[
        "mediana_semanal"
    ].shift(1)

    weekly["precio_lag_2"] = weekly.groupby("canonical_product_id")[
        "mediana_semanal"
    ].shift(2)

    # -----------------------------------------------------------------------
    # Rolling median
    # -----------------------------------------------------------------------

    weekly["mediana_movil"] = weekly.groupby("canonical_product_id")[
        "mediana_semanal"
    ].transform(
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
# Persistence
# ---------------------------------------------------------------------------


def _upsert_feature_snapshot(
    row: dict[str, Any],
    conn: sqlite3.Connection,
) -> None:
    """
    Insert or replace a feature snapshot row.
    """
    conn.execute(
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
        row,
    )


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
        run_at = datetime.now(timezone.utc)

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

    weeks_processed = weekly["week_start"].nunique()

    skus_processed = weekly["canonical_product_id"].nunique()

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

    fx_rates = _fetch_fx_rates(unique_weeks)

    fx_rates_fetched = sum(1 for v in fx_rates.values() if v is not None)

    weekly["usd_uyu_rate"] = weekly["week_start"].map(fx_rates)

    # -----------------------------------------------------------------------
    # Persist
    # -----------------------------------------------------------------------

    rows_written = 0

    with conn:
        for _, row in weekly.iterrows():
            try:
                _upsert_feature_snapshot(
                    {
                        "week_start": row["week_start"],
                        "canonical_product_id": row["canonical_product_id"],
                        "run_at": run_at_str,
                        "precio_lag_1": (
                            None
                            if pd.isna(row["precio_lag_1"])
                            else float(row["precio_lag_1"])
                        ),
                        "precio_lag_2": (
                            None
                            if pd.isna(row["precio_lag_2"])
                            else float(row["precio_lag_2"])
                        ),
                        "mediana_movil": (
                            None
                            if pd.isna(row["mediana_movil"])
                            else float(row["mediana_movil"])
                        ),
                        "dispersion_precios": (
                            None
                            if pd.isna(row["dispersion_precios"])
                            else float(row["dispersion_precios"])
                        ),
                        "usd_uyu_rate": (
                            None
                            if pd.isna(row["usd_uyu_rate"])
                            else float(row["usd_uyu_rate"])
                        ),
                    },
                    conn,
                )

                rows_written += 1

            except Exception as exc:
                msg = (
                    f"Failed to write feature row "
                    f"({row['week_start']}, "
                    f"{row['canonical_product_id']}): {exc}"
                )

                logger.error(msg)

                errors.append(msg)

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
