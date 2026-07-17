import pandas as pd
import pytest

from src.models.evaluation import (
    calculate_metrics,
    evaluate_model_performance,
    walk_forward_cv,
)
from src.models.naive import NaivePersistenceModel


def test_calculate_metrics():
    y_true = pd.Series([100, 200])
    y_pred = pd.Series([110, 190])

    metrics = calculate_metrics(y_true, y_pred)

    assert metrics["mae"] == pytest.approx(10.0)
    assert metrics["rmse"] == pytest.approx(10.0)
    assert metrics["mape"] == pytest.approx(7.5)


def test_walk_forward_cv_basic():
    df = pd.DataFrame({"a": range(20)})
    folds = walk_forward_cv(df, n_splits=4)

    assert len(folds) == 3

    for train_df, test_df in folds:
        assert len(train_df) > 0
        assert len(test_df) > 0
        assert train_df.index.max() < test_df.index.min()


def test_walk_forward_cv_expanding_window():
    df = pd.DataFrame({"a": range(20)})
    folds = walk_forward_cv(df, n_splits=4)

    for i, (train_df, _) in enumerate(folds):
        expected_size = (i + 1) * (20 // 4)
        assert len(train_df) == expected_size


def test_walk_forward_cv_invalid_splits():
    df = pd.DataFrame({"a": range(5)})
    with pytest.raises(ValueError, match="n_splits must be >= 2"):
        walk_forward_cv(df, n_splits=1)
    with pytest.raises(ValueError, match="n_splits=.* exceeds"):
        walk_forward_cv(df, n_splits=10)


def test_naive_persistence_model_missing_column():
    model = NaivePersistenceModel(price_column="non_existent")
    X = pd.DataFrame({"precio_lag_1": [100, 110]})

    with pytest.raises(ValueError, match="Feature 'non_existent' not found in input DataFrame"):
        model.predict(X)


def test_evaluate_model_performance_integration():
    df = pd.DataFrame(
        {
            "precio_lag_1": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            "target": [11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        }
    )

    results = evaluate_model_performance(
        model_factory=NaivePersistenceModel,
        df=df,
        target_col="target",
        feature_cols=["precio_lag_1"],
        n_splits=5,
    )

    assert results.metrics is not None
    assert "mae" in results.metrics
    assert "rmse" in results.metrics
    assert "mape" in results.metrics
    assert len(results.fold_results) == 4


def test_walk_forward_cv_uses_fresh_model_each_fold():
    fit_counts = []

    class FitCountModel:
        def __init__(self):
            self._fitted = False

        def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
            self._fitted = True

        def predict(self, X: pd.DataFrame) -> pd.Series:
            return pd.Series([0.0] * len(X), index=X.index)

    def factory() -> FitCountModel:
        m = FitCountModel()
        fit_counts.append(m)
        return m

    df = pd.DataFrame(
        {
            "f1": range(20),
            "target": range(20),
        }
    )

    results = evaluate_model_performance(
        model_factory=factory,
        df=df,
        target_col="target",
        feature_cols=["f1"],
        n_splits=5,
    )

    assert len(results.fold_results) == 4
    assert len(fit_counts) == 4
    assert all(r.metrics is not None for r in results.fold_results)
    # Verify each fold used a distinct model instance
    for i in range(len(fit_counts) - 1):
        assert fit_counts[i] is not fit_counts[i + 1]


def test_walk_forward_cv_no_state_leak():
    models_created = []

    class LeakDetector:
        def __init__(self):
            self.id = len(models_created)
            self._fitted = False

        def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
            self._fitted = True

        def predict(self, X: pd.DataFrame) -> pd.Series:
            coef = 1.0 + self.id * 0.1
            return pd.Series(X.iloc[:, 0] * coef, index=X.index)

    def factory() -> LeakDetector:
        m = LeakDetector()
        models_created.append(m)
        return m

    df = pd.DataFrame(
        {
            "precio_lag_1": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            "target": [11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        }
    )

    evaluate_model_performance(
        model_factory=factory,
        df=df,
        target_col="target",
        feature_cols=["precio_lag_1"],
        n_splits=4,
    )

    assert len(models_created) >= 2
    for i in range(len(models_created) - 1):
        assert models_created[i] is not models_created[i + 1]
