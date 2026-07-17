"""
Run inference using the latest trained model artifact.

1. Load the latest .joblib artifact from artifacts/
2. Load feature_snapshots for the most recent complete week
3. Generate price predictions
4. Write predictions to the `predictions` table

Usage:
    uv run scripts/run_inference.py
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import joblib
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models.base import PriceModel
from src.storage.schema import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path("data/hardware_pulse.db")
ARTIFACTS_DIR = Path("artifacts")

FEATURE_COLS = [
    "precio_lag_1",
    "precio_lag_2",
    "mediana_movil",
    "dispersion_precios",
    "usd_uyu_rate",
]


def _find_latest_artifact() -> Path:
    joblib_files = sorted(ARTIFACTS_DIR.glob("model_*.joblib"))
    if not joblib_files:
        logger.error("No model artifacts found in %s. Run training first.", ARTIFACTS_DIR)
        sys.exit(1)
    latest = joblib_files[-1]
    logger.info("Found latest artifact: %s", latest)
    return latest


def _load_latest_features(conn) -> pd.DataFrame:
    logger.info("Loading feature snapshots...")
    df = pd.read_sql_query(
        """
        SELECT week_start, canonical_product_id,
               precio_lag_1, precio_lag_2, mediana_movil,
               dispersion_precios, usd_uyu_rate
        FROM feature_snapshots
        ORDER BY week_start DESC
        """,
        conn,
    )
    if df.empty:
        logger.error("No feature snapshots found. Run the feature pipeline first.")
        sys.exit(1)

    latest_week = df["week_start"].iloc[0]
    df_latest = df[df["week_start"] == latest_week].copy()
    logger.info(
        "Using latest week %s: %d rows across %d SKUs",
        latest_week,
        len(df_latest),
        df_latest["canonical_product_id"].nunique(),
    )
    return df_latest


def main() -> None:
    logger.info("=== Inference pipeline start ===")

    model_path = _find_latest_artifact()
    model: PriceModel = joblib.load(model_path)

    meta_path = model_path.with_suffix(".json")
    if meta_path.exists():
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        model_id = metadata.get("model", "unknown")
        logger.info("Loaded model metadata: %s", model_id)
    else:
        model_id = model_path.stem
        logger.info("No metadata found, using artifact stem as model_id")

    conn = init_db(DB_PATH)
    df = _load_latest_features(conn)

    df.sort_values(["canonical_product_id", "week_start"], inplace=True)
    df[FEATURE_COLS] = df.groupby("canonical_product_id")[FEATURE_COLS].ffill()
    nan_mask = df[FEATURE_COLS].isna().any(axis=1)
    dropped_skus = df.loc[nan_mask, "canonical_product_id"].unique().tolist()
    before = len(df)
    df.dropna(subset=FEATURE_COLS, inplace=True)
    dropped = before - len(df)
    if dropped:
        logger.warning(
            "Dropped %d rows with NaN after forward-fill. Affected SKUs: %s",
            dropped,
            dropped_skus,
        )

    if df.empty:
        logger.error("All rows dropped after forward-fill and NaN removal.")
        sys.exit(1)

    X = df[FEATURE_COLS]
    predictions = model.predict(X)

    run_at = datetime.now(UTC).isoformat()
    rows_written = 0

    with conn:
        for idx, pred_val in enumerate(predictions):
            row = df.iloc[idx]
            lower = pred_val * 0.9
            upper = pred_val * 1.1
            conn.execute(
                """
                INSERT OR REPLACE INTO predictions (
                    week_start, canonical_product_id,
                    predicted_price_usd, lower_bound, upper_bound,
                    model_id, run_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["week_start"],
                    row["canonical_product_id"],
                    float(pred_val),
                    float(lower),
                    float(upper),
                    model_id,
                    run_at,
                ),
            )
            rows_written += 1

    logger.info("Wrote %d predictions to database", rows_written)
    logger.info("=== Inference pipeline complete ===")


if __name__ == "__main__":
    main()
