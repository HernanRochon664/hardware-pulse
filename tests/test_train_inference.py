"""Smoke tests for training and inference scripts."""

import pandas as pd
import pytest

from src.models.elasticnet import ElasticNetPriceModel
from src.models.evaluation import evaluate_model_performance


def test_walk_forward_cv_with_elasticnet():
    df = pd.DataFrame(
        {
            "precio_lag_1": range(20),
            "precio_lag_2": range(0, 40, 2),
            "mediana_movil": range(10, 30),
            "dispersion_precios": [1.0] * 20,
            "usd_uyu_rate": [40.0] * 20,
            "target": range(1, 21),
        }
    )

    results = evaluate_model_performance(
        model_factory=ElasticNetPriceModel,
        df=df,
        target_col="target",
        feature_cols=["precio_lag_1", "precio_lag_2", "mediana_movil", "dispersion_precios", "usd_uyu_rate"],
        n_splits=4,
    )

    assert results.metrics is not None
    assert "mae" in results.metrics
    assert "rmse" in results.metrics
    assert "mape" in results.metrics
    assert len(results.fold_results) == 3
