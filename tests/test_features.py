"""Tests for the feature engineering pipeline.

Covers the weekly aggregation logic in `_compute_weekly_features`.
"""

from datetime import UTC, datetime

import pandas as pd

from src.pipelines.features import _compute_weekly_features


def test_compute_weekly_features_empty_dataframe() -> None:
    """Return an empty DataFrame when no price snapshots are available."""
    df = pd.DataFrame(columns=["timestamp", "canonical_product_id", "price_usd", "source"])

    result = _compute_weekly_features(df)

    assert result.empty


def test_compute_weekly_features_single_week_no_lags() -> None:
    """Compute a single weekly row and ensure lags are NaN."""
    df = pd.DataFrame(
        {
            "timestamp": [datetime(2026, 4, 6, 12, 0, tzinfo=UTC)],
            "canonical_product_id": ["sku-1"],
            "price_usd": [100.0],
            "source": ["thot"],
        }
    )

    result = _compute_weekly_features(df)

    assert len(result) == 1
    assert result.loc[0, "week_start"] == "2026-04-06"
    assert result.loc[0, "mediana_semanal"] == 100.0
    assert pd.isna(result.loc[0, "precio_lag_1"])
    assert pd.isna(result.loc[0, "precio_lag_2"])
    assert result.loc[0, "mediana_movil"] == 100.0
    assert pd.isna(result.loc[0, "dispersion_precios"])


def test_compute_weekly_features_with_gap_lag_is_nan() -> None:
    """Gap weeks are filled with NaN so shift() is calendar-date-aware."""
    df = pd.DataFrame(
        {
            "timestamp": [
                datetime(2026, 4, 6, 10, 0, tzinfo=UTC),
                datetime(2026, 4, 6, 11, 0, tzinfo=UTC),
                datetime(2026, 4, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 4, 20, 11, 0, tzinfo=UTC),
            ],
            "canonical_product_id": ["sku-1", "sku-1", "sku-1", "sku-1"],
            "price_usd": [100.0, 120.0, 110.0, 130.0],
            "source": ["thot", "pccompu", "thot", "pccompu"],
        }
    )

    result = _compute_weekly_features(df)

    # Gap week 2026-04-13 is now filled with NaN
    assert len(result) == 3
    assert list(result["week_start"]) == ["2026-04-06", "2026-04-13", "2026-04-20"]

    first_row = result.iloc[0]
    gap_row = result.iloc[1]
    last_row = result.iloc[2]

    assert first_row["mediana_semanal"] == 110.0
    assert pd.isna(first_row["precio_lag_1"])
    assert pd.isna(first_row["precio_lag_2"])
    assert first_row["mediana_movil"] == 110.0

    assert pd.isna(gap_row["mediana_semanal"])
    assert gap_row["precio_lag_1"] == 110.0
    assert pd.isna(gap_row["precio_lag_2"])

    assert last_row["mediana_semanal"] == 120.0
    assert pd.isna(last_row["precio_lag_1"])
    assert last_row["precio_lag_2"] == 110.0
    assert last_row["mediana_movil"] == 115.0
