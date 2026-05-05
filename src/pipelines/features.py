"""
Feature engineering pipeline for hardware-pulse.

Responsibilities:
- Read resolved price data from price_snapshots
- Compute weekly median prices per canonical SKU
- Compute lag features and rolling median (time-series features)
- Compute price dispersion across sources per week/SKU
- Fetch weekly USD/UYU exchange rate from Frankfurter API
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

FRANKFURTER_URL = "https://api.frankfurter.app/{date}?from=USD&to=UYU"
ROLLING_WINDOW = 4  # weeks for rolling median
REQUEST_TIMEOUT = 10  # seconds for Frankfurter API calls

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


def _fetch_usd_uyu_rate(date_str: str) -> float | None:
    """
    Fetch the USD/UYU exchange rate for a given date from Frankfurter API.

    Args:
        date_str: Date in YYYY-MM-DD format (Monday of the target week).

    Returns:
        Exchange rate as float, or None if the request fails.
    """
    url = FRANKFURTER_URL.format(date=date_str)
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        rate = data["rates"]["UYU"]
        logger.debug("FX rate for %s: %.4f", date_str, rate)
        return float(rate)
    except Exception as exc:
        logger.warning("Failed to fetch FX rate for %s: %s", date_str, exc)
        return None


def _fetch_fx_rates(week_starts: list[str]) -> dict[str, float | None]:
    """
    Fetch USD/UYU rates for all unique week_start dates.

    Args:
        week_starts: List of date strings (YYYY-MM-DD), one per unique week.

    Returns:
        Dict mapping date string → exchange rate (or None on failure).
    """
    rates: dict[str, float | None] = {}
    for date_str in sorted(set(week_starts)):
        rates[date_str] = _fetch_usd_uyu_rate(date_str)
    return rates


# ---------------------------------------------------------------------------
# Core computations
# ---------------------------------------------------------------------------


def _load_price_snapshots(
    conn: sqlite3.Connection,
    since: datetime | None,
) -> pd.DataFrame:
    """
    Load price_snapshots from the database into a DataFrame.

    Args:
        conn:  Open SQLite connection.
        since: If provided, only load snapshots at or after this timestamp.

    Returns:
        DataFrame with columns: timestamp, canonical_product_id, price_usd, source.
    """
    query = """
        SELECT timestamp, canonical_product_id, price_usd, source
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
    Compute weekly features from price snapshot data.

    Steps:
    1. Derive week_start (Monday) from each timestamp
    2. Compute weekly median price per (week_start, SKU)
    3. Compute price dispersion (std) per (week_start, SKU) across sources
    4. Sort by (SKU, week_start) and compute lag_1, lag_2, rolling median
       — no forward fill: NaN where there is no prior observation

    Args:
        df: Raw price snapshots with columns: timestamp, canonical_product_id,
            price_usd, source.

    Returns:
        DataFrame with one row per (week_start, canonical_product_id) and
        feature columns: mediana_semanal, dispersion_precios,
        precio_lag_1, precio_lag_2, mediana_movil.
    """
    if df.empty:
        return pd.DataFrame()

    # Step 1: derive week_start (normalize to Monday 00:00 UTC)
    df = df.copy()
    df["week_start"] = df["timestamp"].dt.normalize() - pd.to_timedelta(
        df["timestamp"].dt.weekday, unit="D"
    )

    # Step 2: weekly median and dispersion per SKU
    weekly = (
        df.groupby(["week_start", "canonical_product_id"])["price_usd"]
        .agg(mediana_semanal="median", dispersion_precios="std")
        .reset_index()
    )

    # Step 3: sort and compute lag/rolling features per SKU group
    weekly = weekly.sort_values(["canonical_product_id", "week_start"])

    weekly["precio_lag_1"] = weekly.groupby("canonical_product_id")[
        "mediana_semanal"
    ].shift(1)

    weekly["precio_lag_2"] = weekly.groupby("canonical_product_id")[
        "mediana_semanal"
    ].shift(2)

    weekly["mediana_movil"] = weekly.groupby("canonical_product_id")[
        "mediana_semanal"
    ].transform(lambda s: s.rolling(window=ROLLING_WINDOW, min_periods=1).median())

    # Format week_start as ISO date string for SQLite storage
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
    Insert or replace a single feature snapshot row.

    Uses INSERT OR REPLACE to handle the UNIQUE (week_start, canonical_product_id)
    constraint — on conflict, the existing row is replaced with new values.

    Args:
        row:  Dict with all feature_snapshots columns.
        conn: Open SQLite connection (caller manages transaction).
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO feature_snapshots (
            week_start, canonical_product_id, run_at,
            precio_lag_1, precio_lag_2, mediana_movil,
            dispersion_precios, usd_uyu_rate
        ) VALUES (
            :week_start, :canonical_product_id, :run_at,
            :precio_lag_1, :precio_lag_2, :mediana_movil,
            :dispersion_precios, :usd_uyu_rate
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
    Run the full feature engineering pipeline.

    Reads resolved price snapshots, computes weekly features per SKU,
    fetches USD/UYU exchange rates, and persists results to feature_snapshots.

    Args:
        conn:   Open SQLite connection (from storage.schema.init_db).
        since:  If provided, only process snapshots at or after this datetime.
                Useful for incremental re-runs.
        run_at: Timestamp for this pipeline run. Defaults to now (UTC).

    Returns:
        FeatureResult with run statistics and any errors encountered.
    """
    if run_at is None:
        run_at = datetime.now(timezone.utc)

    run_at_str = run_at.isoformat()
    errors: list[str] = []

    # --- Load data ---
    logger.info("Loading price snapshots (since=%s)", since)
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

    # --- Compute features ---
    logger.info("Computing weekly features for %d snapshots", len(df))
    weekly = _compute_weekly_features(df)

    weeks_processed = weekly["week_start"].nunique()
    skus_processed = weekly["canonical_product_id"].nunique()
    logger.info(
        "Computed features: %d weeks × %d SKUs = %d rows",
        weeks_processed,
        skus_processed,
        len(weekly),
    )

    # --- Fetch exchange rates ---
    unique_weeks = weekly["week_start"].tolist()
    logger.info("Fetching FX rates for %d unique weeks", len(set(unique_weeks)))
    fx_rates = _fetch_fx_rates(unique_weeks)
    fx_rates_fetched = sum(1 for v in fx_rates.values() if v is not None)

    weekly["usd_uyu_rate"] = weekly["week_start"].map(fx_rates)

    # --- Persist ---
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
                            if pd.isna(row.get("usd_uyu_rate"))
                            else float(row["usd_uyu_rate"])
                        ),
                    },
                    conn,
                )
                rows_written += 1
            except Exception as exc:
                msg = (
                    f"Failed to write feature row "
                    f"({row['week_start']}, {row['canonical_product_id']}): {exc}"
                )
                logger.error(msg)
                errors.append(msg)

    logger.info("Feature pipeline complete: %d rows written", rows_written)

    return FeatureResult(
        run_at=run_at_str,
        weeks_processed=weeks_processed,
        skus_processed=skus_processed,
        rows_written=rows_written,
        fx_rates_fetched=fx_rates_fetched,
        errors=errors,
    )
