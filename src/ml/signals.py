"""
Buy/wait signal orchestration: train, gate, register and publish.

A model is published (``is_active = 1``) only when its walk-forward backtest
beats the baseline: positive Brier skill and a PR-AUC above the drop base
rate. Signals are written only for products with at least
``MIN_HISTORY_DAYS`` of price history; otherwise the dashboard shows
"Señal no disponible".
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta

import numpy as np

from src.ml.backtest import WAIT_THRESHOLD, walk_forward
from src.ml.dataset import LabelSpec, build_dataset, load_panel
from src.ml.models import LogisticModel
from src.settings import from_iso, to_iso, utcnow

logger = logging.getLogger(__name__)

RETRAIN_AFTER = timedelta(days=7)
MIN_HISTORY_DAYS = 21
MIN_TEST_ROWS = 200
SPEC = LabelSpec()


def _clean_start(conn: sqlite3.Connection) -> datetime | None:
    """Local midnight of the first v2 scrape: rows before it come from the v1 history."""
    row = conn.execute(
        "SELECT MIN(valid_from) FROM price_periods WHERE origin = 'scrape'"
    ).fetchone()
    if not row or not row[0]:
        return None
    day = from_iso(row[0]).date()
    return datetime(day.year, day.month, day.day)


def passes_gate(metrics: dict) -> tuple[bool, str]:
    n_test = metrics.get("n_test", 0)
    if n_test < MIN_TEST_ROWS:
        return False, f"backtest con pocos ejemplos ({n_test})"
    skill, pr_auc, base = metrics.get("brier_skill"), metrics.get("pr_auc"), metrics["base_rate"]
    if skill is None or skill <= 0:
        return False, f"Brier skill {_fmt(skill)}: no le gana al baseline"
    if pr_auc is None or pr_auc <= base:
        return False, f"PR-AUC {_fmt(pr_auc)} no supera la tasa base {base:.3f}"
    return True, "ok"


def train_and_register(conn: sqlite3.Connection, now: datetime | None = None) -> dict | None:
    """Backtest, fit on all labelled rows and store the model; active only if gated in."""
    now = now or utcnow()
    data = build_dataset(load_panel(conn), SPEC)
    labelled = data.loc[data["label"].notna()]
    if labelled.empty or labelled["label"].nunique() < 2:
        logger.info("ML: not enough labelled history to train (%d rows)", len(labelled))
        return None
    backtest = walk_forward(data, SPEC, clean_start=_clean_start(conn))
    ok, reason = passes_gate(backtest.metrics)
    model = LogisticModel.fit(labelled)
    version = f"logreg-{now.strftime('%Y%m%dT%H%M%S')}"
    params = {
        **model.to_dict(),
        "horizon_days": SPEC.horizon_days,
        "drop": SPEC.drop,
        "wait_threshold": WAIT_THRESHOLD,
        "train_rows": len(labelled),
        "train_from": labelled["date"].min().strftime("%Y-%m-%d"),
        "train_to": labelled["date"].max().strftime("%Y-%m-%d"),
    }
    metrics = {**backtest.metrics, "gate_passed": ok, "gate_reason": reason}
    with conn:
        if ok:
            conn.execute("UPDATE model_registry SET is_active = 0")
        conn.execute(
            "INSERT INTO model_registry (version, trained_at, kind, params_json, metrics_json, "
            "is_active) VALUES (?, ?, 'logistic', ?, ?, ?)",
            (version, to_iso(now), json.dumps(params), json.dumps(metrics), int(ok)),
        )
    logger.info(
        "ML: %s trained on %d rows · PR-AUC %s (baseline %s, base rate %s) · "
        "Brier skill %s · gate: %s",
        version,
        len(labelled),
        _fmt(metrics.get("pr_auc")),
        _fmt(metrics.get("baseline_pr_auc")),
        _fmt(metrics.get("base_rate")),
        _fmt(metrics.get("brier_skill")),
        reason,
    )
    return {"version": version, "params": params, "metrics": metrics, "active": ok}


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def train_if_stale(conn: sqlite3.Connection, now: datetime | None = None) -> None:
    now = now or utcnow()
    row = conn.execute("SELECT MAX(trained_at) FROM model_registry").fetchone()
    if row and row[0] and now - from_iso(row[0]) < RETRAIN_AFTER:
        logger.info("ML: last training %s is recent, skipping", row[0])
        return
    train_and_register(conn, now)


def active_model(conn: sqlite3.Connection) -> tuple[str, LogisticModel, dict] | None:
    row = conn.execute(
        "SELECT version, params_json FROM model_registry WHERE is_active = 1 "
        "ORDER BY trained_at DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    params = json.loads(row[1])
    return row[0], LogisticModel.from_dict(params), params


def recommendation(p_drop: float, threshold: float) -> str:
    return "esperar" if p_drop >= threshold else "comprar"


def publish_signals(conn: sqlite3.Connection) -> int:
    """Replace the signals table with today's prediction for every eligible product."""
    loaded = active_model(conn)
    with conn:
        conn.execute("DELETE FROM signals")
    if loaded is None:
        logger.info("ML: no published model, signals cleared")
        return 0
    version, model, params = loaded
    data = build_dataset(load_panel(conn), SPEC)
    if data.empty:
        return 0
    latest = data["date"].max()
    eligible = (
        (data["date"] == latest)
        & (data["history_days"] >= MIN_HISTORY_DAYS)
        & data["product_id"].isin(list(_products_with_offer(conn)))
    )
    current = data.loc[eligible]
    if current.empty:
        return 0
    probs = model.predict_proba(current)
    contrib = model.contributions(current)
    threshold = float(params.get("wait_threshold", WAIT_THRESHOLD))
    horizon = int(params.get("horizon_days", SPEC.horizon_days))
    as_of = latest.strftime("%Y-%m-%d")
    rows = []
    for i, record in enumerate(current.to_dict(orient="records")):
        top = np.argsort(-np.abs(contrib[i]))[:3]
        details = {
            "price_usd": float(record["price"]),
            "drivers": [
                {"feature": model.features[j], "contribution": round(float(contrib[i][j]), 3)}
                for j in top
            ],
        }
        p_drop = float(probs[i])
        rows.append(
            (
                "product",
                str(int(record["product_id"])),
                as_of,
                horizon,
                round(p_drop, 4),
                recommendation(p_drop, threshold),
                version,
                json.dumps(details),
            )
        )
    with conn:
        conn.executemany(
            "INSERT INTO signals (entity_type, entity_id, as_of, horizon_days, p_drop, "
            "recommendation, model_version, details_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    logger.info("ML: published %d signals with %s", len(rows), version)
    return len(rows)


def _products_with_offer(conn: sqlite3.Connection) -> set[int]:
    rows = conn.execute("SELECT product_id FROM product_stats WHERE best_price_usd IS NOT NULL")
    return {int(r[0]) for r in rows}
