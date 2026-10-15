"""Buy/wait signal: point-in-time features, labels, gate and JSON persistence."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from src.ml import signals
from src.ml.backtest import walk_forward
from src.ml.dataset import FEATURES, LabelSpec, build_dataset, load_panel
from src.ml.models import LogisticModel
from src.storage.db import connect

START = pd.Timestamp("2026-06-01")
NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def _panel(prices: dict[int, list[float]], category: str = "gpu") -> pd.DataFrame:
    rows = []
    for product_id, series in prices.items():
        for day, price in enumerate(series):
            rows.append(
                {
                    "product_id": product_id,
                    "date": START + pd.Timedelta(days=day),
                    "price": price,
                    "n_offers": 2,
                    "n_stores": 2,
                    "category": category,
                    "family_id": "gpu:test",
                    "family_median": price,
                }
            )
    return pd.DataFrame(rows)


def test_label_marks_days_where_waiting_pays_off():
    series = [100.0] * 20 + [90.0] * 20  # 10% drop on day 20
    data = build_dataset(_panel({1: series}), LabelSpec(horizon_days=14, drop=0.05))
    label = data.set_index("date")["label"]
    assert label[START + pd.Timedelta(days=5)] == 0  # drop is 15 days away
    assert label[START + pd.Timedelta(days=6)] == 1  # drop lands on day 6 + 14
    assert label[START + pd.Timedelta(days=19)] == 1
    assert label[START + pd.Timedelta(days=20)] == 0  # already at the low
    assert np.isnan(label[START + pd.Timedelta(days=30)])  # window not complete yet


def test_features_do_not_look_into_the_future():
    rng = np.random.default_rng(0)
    series = list(100 + rng.normal(0, 3, 120).cumsum())
    cutoff = 70
    altered = series[:cutoff] + [p * rng.uniform(0.5, 1.5) for p in series[cutoff:]]
    a = build_dataset(_panel({1: series}))
    b = build_dataset(_panel({1: altered}))
    past = a["date"] < START + pd.Timedelta(days=cutoff)
    pd.testing.assert_frame_equal(
        a.loc[past, FEATURES].reset_index(drop=True), b.loc[past, FEATURES].reset_index(drop=True)
    )


def test_gaps_keep_calendar_lookbacks_honest():
    panel = _panel({1: [100.0] * 10 + [80.0] * 10})
    short_gap = panel[
        (panel["date"] < START + pd.Timedelta(days=3))
        | (panel["date"] >= START + pd.Timedelta(days=12))
    ]
    long_gap = panel[
        (panel["date"] < START + pd.Timedelta(days=2))
        | (panel["date"] >= START + pd.Timedelta(days=12))
    ]
    day12 = START + pd.Timedelta(days=12)
    # 7 days before day 12 is day 5: within the 3-day carry-forward of day 2, beyond that of day 1
    assert build_dataset(short_gap).set_index("date").loc[day12, "ret_7"] == pytest.approx(-0.2)
    assert build_dataset(long_gap).set_index("date").loc[day12, "ret_7"] == 0.0  # unknown


def test_logistic_model_round_trips_through_json():
    rng = np.random.default_rng(1)
    frame = pd.DataFrame(rng.normal(size=(300, len(FEATURES))), columns=FEATURES)
    frame["label"] = (frame["ret_7"] + rng.normal(0, 0.5, 300) > 0).astype(float)
    model = LogisticModel.fit(frame)
    restored = LogisticModel.from_dict(json.loads(json.dumps(model.to_dict())))
    np.testing.assert_allclose(model.predict_proba(frame), restored.predict_proba(frame))


def test_gate_rejects_models_that_do_not_beat_the_baseline():
    base = {"n_test": 5000, "base_rate": 0.05, "pr_auc": 0.2, "brier_skill": 0.05}
    assert signals.passes_gate(base)[0]
    assert not signals.passes_gate({**base, "brier_skill": -0.1})[0]
    assert not signals.passes_gate({**base, "pr_auc": 0.04})[0]
    assert not signals.passes_gate({**base, "n_test": 50})[0]


def test_walk_forward_never_trains_on_rows_whose_label_overlaps_the_test_window():
    rng = np.random.default_rng(2)
    prices = {pid: list(100 + rng.normal(0, 2, 150).cumsum()) for pid in range(1, 15)}
    data = build_dataset(_panel(prices))
    result = walk_forward(data, LabelSpec())
    assert result.metrics["n_test"] > 0
    first_test = pd.Timestamp(result.metrics["test_from"])
    assert first_test >= data["date"].min() + pd.Timedelta(days=45 + 14)


# --- database round trip -----------------------------------------------------


@pytest.fixture
def ml_db():
    conn = connect(":memory:")
    stamp = "2026-10-15T12:00:00Z"
    conn.execute("INSERT INTO families (id, category, name) VALUES ('gpu:test', 'gpu', 'Test')")
    rng = np.random.default_rng(3)
    for pid in range(1, 6):
        conn.execute(
            "INSERT INTO products (id, fingerprint, category, family_id, name, created_at, "
            "updated_at) VALUES (?, ?, 'gpu', 'gpu:test', ?, ?, ?)",
            (pid, f"fp{pid}", f"GPU {pid}", stamp, stamp),
        )
        days = 40 if pid != 5 else 10  # product 5 lacks history
        start = 40 - days
        for d in range(start, 40):
            day = (START + pd.Timedelta(days=d)).strftime("%Y-%m-%d")
            conn.execute(
                "INSERT INTO product_daily VALUES (?, ?, ?, 1, 1)",
                (pid, day, float(100 + rng.normal(0, 1))),
            )
        conn.execute(
            "INSERT INTO product_stats (product_id, best_price_usd, n_offers, n_stores, "
            "updated_at) VALUES (?, 100, 1, 1, ?)",
            (pid, stamp),
        )
    conn.commit()
    yield conn
    conn.close()


def _register(conn, *, intercept: float, active: bool) -> None:
    params = {
        "features": FEATURES,
        "mean": [0.0] * len(FEATURES),
        "scale": [1.0] * len(FEATURES),
        "coef": [0.0] * len(FEATURES),
        "intercept": intercept,
        "horizon_days": 14,
        "wait_threshold": 0.5,
    }
    conn.execute(
        "INSERT INTO model_registry VALUES (?, '2026-10-15T00:00:00Z', 'logistic', ?, '{}', ?)",
        (f"v{intercept}", json.dumps(params), int(active)),
    )
    conn.commit()


def test_no_published_model_means_no_signals(ml_db):
    _register(ml_db, intercept=2.0, active=False)
    assert signals.publish_signals(ml_db) == 0
    assert ml_db.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 0


def test_published_model_writes_signals_only_with_enough_history(ml_db):
    _register(ml_db, intercept=2.0, active=True)  # p ≈ 0.88 -> wait
    assert signals.publish_signals(ml_db) == 4
    rows = ml_db.execute("SELECT entity_id, recommendation, p_drop FROM signals").fetchall()
    assert {r["entity_id"] for r in rows} == {"1", "2", "3", "4"}  # product 5 has 10 days
    assert {r["recommendation"] for r in rows} == {"esperar"}
    assert all(r["p_drop"] == pytest.approx(0.8808, abs=1e-3) for r in rows)


def test_untrainable_history_registers_nothing(ml_db):
    assert signals.train_and_register(ml_db, NOW) is None  # labels are all one class or empty
    assert ml_db.execute("SELECT COUNT(*) FROM model_registry").fetchone()[0] == 0


def test_load_panel_reads_product_daily(ml_db):
    panel = load_panel(ml_db)
    assert set(panel["product_id"]) == {1, 2, 3, 4, 5}
    assert {"price", "category", "family_median"} <= set(panel.columns)
