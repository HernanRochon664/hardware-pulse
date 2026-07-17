"""
Train the best price prediction model and persist it as a joblib artifact.

1. Load feature snapshots and compute weekly median targets from prices
2. Filter SKUs with >= 12 weeks of history
3. Forward-fill lag features per SKU, drop remaining NaN rows
4. Evaluate ElasticNetPriceModel and NaivePersistenceModel via walk-forward CV
5. Compare MAPE across folds, select winner
6. Re-train winner on full data and persist to artifacts/

Usage:
    uv run scripts/run_train.py
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.elasticnet import ElasticNetPriceModel
from src.models.evaluation import calculate_metrics, evaluate_model_performance, walk_forward_cv
from src.models.naive import NaivePersistenceModel
from src.storage.schema import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")
ARTIFACTS_DIR = Path("artifacts")
MIN_WEEKS = 12
CV_SPLITS = 5

FEATURE_COLS = [
    "precio_lag_1",
    "precio_lag_2",
    "mediana_movil",
    "dispersion_precios",
    "usd_uyu_rate",
]


def _load_features(conn) -> pd.DataFrame:
    logger.info("Loading feature snapshots...")
    df = pd.read_sql_query(
        """
        SELECT week_start, canonical_product_id,
               precio_lag_1, precio_lag_2, mediana_movil,
               dispersion_precios, usd_uyu_rate
        FROM feature_snapshots
        ORDER BY canonical_product_id, week_start
        """,
        conn,
    )
    logger.info("Loaded %d feature rows", len(df))
    return df


def _load_targets(conn) -> pd.DataFrame:
    logger.info("Computing weekly median targets from price_snapshots...")
    df = pd.read_sql_query(
        """
        SELECT timestamp, canonical_product_id, price_usd
        FROM price_snapshots
        ORDER BY canonical_product_id, timestamp
        """,
        conn,
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["week_start"] = df["timestamp"].dt.normalize() - pd.to_timedelta(
        df["timestamp"].dt.weekday, unit="D"
    )
    weekly = df.groupby(["week_start", "canonical_product_id"])["price_usd"].median().reset_index()
    weekly.rename(columns={"price_usd": "target"}, inplace=True)
    weekly["week_start"] = weekly["week_start"].dt.strftime("%Y-%m-%d")
    logger.info("Computed %d weekly targets", len(weekly))
    return weekly


def _merge_and_filter(features: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    merged = features.merge(targets, on=["week_start", "canonical_product_id"], how="inner")
    logger.info("Merged features + targets: %d rows", len(merged))

    sku_weeks = merged.groupby("canonical_product_id").size()
    valid_skus = sku_weeks[sku_weeks >= MIN_WEEKS].index
    filtered = merged[merged["canonical_product_id"].isin(valid_skus)].copy()
    logger.info(
        "Filtered to SKUs with >= %d weeks: %d SKUs, %d rows",
        MIN_WEEKS,
        len(valid_skus),
        len(filtered),
    )
    return filtered


def _ffill_and_dropna(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Forward-fill lag features per SKU, then drop remaining NaN rows."""
    result = df.copy()
    result.sort_values(["canonical_product_id", "week_start"], inplace=True)
    result[feature_cols] = result.groupby("canonical_product_id")[feature_cols].ffill()
    before = len(result)
    result.dropna(subset=feature_cols, inplace=True)
    dropped = before - len(result)
    if dropped:
        logger.info("Dropped %d rows with NaN after forward-fill", dropped)
    return result


def main() -> None:
    logger.info("=== Training pipeline start ===")

    conn = init_db(DB_PATH)

    features = _load_features(conn)
    if features.empty:
        logger.error("No feature snapshots found. Run the feature pipeline first.")
        sys.exit(1)

    targets = _load_targets(conn)
    if targets.empty:
        logger.error("No price snapshots found. Run the resolve pipeline first.")
        sys.exit(1)

    data = _merge_and_filter(features, targets)
    if data.empty:
        logger.error(
            "No SKUs with >= %d weeks of data. Collect more data or reduce MIN_WEEKS.",
            MIN_WEEKS,
        )
        sys.exit(1)

    data = _ffill_and_dropna(data, FEATURE_COLS)
    if data.empty:
        logger.error("All rows dropped after forward-fill and NaN removal.")
        sys.exit(1)

    data.sort_values(["week_start", "canonical_product_id"], inplace=True)

    logger.info("Evaluating ElasticNetPriceModel via walk-forward CV...")
    en_results = evaluate_model_performance(
        model_factory=ElasticNetPriceModel,
        df=data,
        target_col="target",
        feature_cols=FEATURE_COLS,
        n_splits=CV_SPLITS,
    )
    en_metrics = en_results.metrics
    logger.info(
        "ElasticNet  | MAE=%.2f | RMSE=%.2f | MAPE=%.2f%%",
        en_metrics["mae"],
        en_metrics["rmse"],
        en_metrics["mape"],
    )

    logger.info("Evaluating NaivePersistenceModel via walk-forward CV...")
    na_results = evaluate_model_performance(
        model_factory=NaivePersistenceModel,
        df=data,
        target_col="target",
        feature_cols=FEATURE_COLS,
        n_splits=CV_SPLITS,
    )
    na_metrics = na_results.metrics
    logger.info(
        "NaivePersistence | MAE=%.2f | RMSE=%.2f | MAPE=%.2f%%",
        na_metrics["mae"],
        na_metrics["rmse"],
        na_metrics["mape"],
    )

    if en_metrics["mape"] <= na_metrics["mape"]:
        winner = ElasticNetPriceModel()
        winner_name = "ElasticNetPriceModel"
        winner_metrics = en_metrics
        logger.info(
            "Winner: ElasticNetPriceModel (MAPE %.2f%% < %.2f%%)",
            en_metrics["mape"],
            na_metrics["mape"],
        )
    else:
        winner = NaivePersistenceModel()
        winner_name = "NaivePersistenceModel"
        winner_metrics = na_metrics
        logger.info(
            "Winner: NaivePersistenceModel (MAPE %.2f%% < %.2f%%)",
            na_metrics["mape"],
            en_metrics["mape"],
        )

    today_str = datetime.now(UTC).strftime("%Y%m%d")
    model_path = ARTIFACTS_DIR / f"model_{today_str}.joblib"

    X_full = data[FEATURE_COLS]
    y_full = data["target"]
    winner.fit(X_full, y_full)
    winner.save(model_path)

    metadata = {
        "model": winner_name,
        "train_date": today_str,
        "train_rows": len(data),
        "skus": int(data["canonical_product_id"].nunique()),
        "features": FEATURE_COLS,
        "metrics_elasticnet": en_metrics,
        "metrics_naive": na_metrics,
        "metrics_winner": winner_metrics,
    }
    meta_path = ARTIFACTS_DIR / f"model_{today_str}.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    logger.info("Saved model artifact to %s", model_path)
    logger.info("Saved metadata to %s", meta_path)
    logger.info("=== Training pipeline complete ===")


if __name__ == "__main__":
    main()
