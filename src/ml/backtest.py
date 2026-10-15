"""
Walk-forward backtest of the buy/wait models.

Test windows of ``step_days`` move forward in time. Each one is scored by
models trained only on rows whose label was already known before the window
started: a row dated ``d`` needs prices up to ``d + horizon``, so training
stops ``horizon`` days before the test window (the purge).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from src.ml.dataset import LabelSpec
from src.ml.models import BaselineModel, LogisticModel

MIN_TRAIN_DAYS = 45
MIN_TRAIN_ROWS = 500
WAIT_THRESHOLD = 0.5


@dataclass
class BacktestResult:
    predictions: pd.DataFrame  # date, product_id, label, p_model, p_baseline, saving_if_wait
    metrics: dict


def walk_forward(
    data: pd.DataFrame,
    spec: LabelSpec,
    *,
    step_days: int = 14,
    clean_start: datetime | None = None,
) -> BacktestResult:
    labelled = data.loc[data["label"].notna()].sort_values(by=["date"])
    if labelled.empty:
        return BacktestResult(pd.DataFrame(), {"n_test": 0})
    first, last = labelled["date"].min(), labelled["date"].max()
    purge = pd.Timedelta(days=spec.horizon_days)
    start = first + pd.Timedelta(days=MIN_TRAIN_DAYS) + purge
    chunks = []
    while start <= last:
        end = start + pd.Timedelta(days=step_days)
        train = labelled.loc[labelled["date"] < start - purge]
        test = labelled.loc[(labelled["date"] >= start) & (labelled["date"] < end)]
        if len(train) >= MIN_TRAIN_ROWS and train["label"].nunique() == 2 and not test.empty:
            model = LogisticModel.fit(train)
            baseline = BaselineModel.fit(train)
            chunk = test[["date", "product_id", "category", "label", "price", "end_price"]].copy()
            chunk["p_model"] = model.predict_proba(test)
            chunk["p_baseline"] = baseline.predict_proba(test)
            chunks.append(chunk)
        start = end
    if not chunks:
        return BacktestResult(pd.DataFrame(), {"n_test": 0})
    preds = pd.concat(chunks, ignore_index=True)
    # waiting means buying at the end of the horizon instead of today
    preds["saving_if_wait"] = (preds["price"] - preds["end_price"]) / preds["price"]
    metrics = score(preds)
    if clean_start is not None:
        metrics["by_period"] = {
            "legacy": score(preds.loc[preds["date"] < clean_start]),
            "clean": score(preds.loc[preds["date"] >= clean_start]),
        }
    metrics["test_from"] = preds["date"].min().strftime("%Y-%m-%d")
    metrics["test_to"] = preds["date"].max().strftime("%Y-%m-%d")
    return BacktestResult(preds, metrics)


def _brier(p: np.ndarray, y: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def calibration(p: np.ndarray, y: np.ndarray, bins: int = 10) -> list[dict]:
    edges = np.linspace(0, 1, bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1]), 0, bins - 1)
    table = []
    for b in range(bins):
        mask = idx == b
        if mask.any():
            table.append(
                {
                    "bin": f"{edges[b]:.0%}–{edges[b + 1]:.0%}",
                    "predicted": round(float(p[mask].mean()), 3),
                    "observed": round(float(y[mask].mean()), 3),
                    "n": int(mask.sum()),
                }
            )
    return table


def score(preds: pd.DataFrame) -> dict:
    from sklearn.metrics import average_precision_score

    n = len(preds)
    if n == 0:
        return {"n_test": 0}
    y = preds["label"].to_numpy(dtype=float)
    p = preds["p_model"].to_numpy(dtype=float)
    b = preds["p_baseline"].to_numpy(dtype=float)
    both = y.min() != y.max()
    brier, brier_base = _brier(p, y), _brier(b, y)
    wait = p >= WAIT_THRESHOLD
    saving = preds["saving_if_wait"].fillna(0.0).to_numpy(dtype=float)
    return {
        "n_test": n,
        "base_rate": float(y.mean()),
        "pr_auc": float(average_precision_score(y, p)) if both else None,
        "baseline_pr_auc": float(average_precision_score(y, b)) if both else None,
        "brier": brier,
        "baseline_brier": brier_base,
        "brier_skill": 1 - brier / brier_base if brier_base > 0 else None,
        "wait_share": float(wait.mean()),
        # average gain per decision when following the signal (0 when it says buy)
        "avg_saving": float(np.where(wait, saving, 0.0).mean()),
        "calibration": calibration(p, y),
    }
