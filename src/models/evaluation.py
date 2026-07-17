"""
Evaluation module for price prediction models.

This module provides tools to split time-series data using walk-forward
cross-validation, calculate regression metrics (MAE, RMSE, MAPE), and
execute a full evaluation pipeline with averaged metrics across folds.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

from .base import PriceModel


@dataclass
class EvaluationResult:
    metrics: dict[str, float]
    predictions: pd.Series
    actuals: pd.Series


@dataclass
class CrossValidationResult:
    metrics: dict[str, float]
    fold_results: list[EvaluationResult] = field(default_factory=list)


def calculate_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
    mask = y_true != 0
    mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(root_mean_squared_error(y_true, y_pred)),
        "mape": float(mape),
    }


def walk_forward_cv(
    df: pd.DataFrame,
    n_splits: int = 5,
) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Generate walk-forward cross-validation splits.

    Each fold uses an expanding training window to predict the next segment,
    preserving chronological order and preventing data leakage.

    Args:
        df: DataFrame sorted chronologically.
        n_splits: Number of CV folds (default 5).

    Returns:
        List of (train_df, test_df) tuples, one per fold.
    """
    if n_splits < 2:
        raise ValueError("n_splits must be >= 2")
    n = len(df)
    if n < n_splits:
        raise ValueError(f"n_splits={n_splits} exceeds number of rows ({n})")

    fold_size = n // n_splits
    folds = []

    for i in range(1, n_splits):
        train_end = i * fold_size
        test_end = (i + 1) * fold_size if i < n_splits - 1 else n
        train_df = df.iloc[:train_end]
        test_df = df.iloc[train_end:test_end]
        folds.append((train_df, test_df))

    return folds


def evaluate_model_performance(
    model_factory: Callable[[], PriceModel],
    df: pd.DataFrame,
    target_col: str,
    feature_cols: list[str],
    n_splits: int = 5,
) -> CrossValidationResult:
    """
    Evaluate model with walk-forward CV, averaging metrics over folds.

    For each fold: expanding window train -> predict next segment -> compute metrics.
    Returns averaged metrics across all folds.

    Args:
        model_factory: Callable that returns a fresh PriceModel instance per fold.
        df: DataFrame containing features and target, sorted chronologically.
        target_col: Name of the target price column.
        feature_cols: List of columns to be used as features for X.
        n_splits: Number of CV folds (default 5).

    Returns:
        CrossValidationResult with averaged metrics and per-fold results.
    """
    folds = walk_forward_cv(df, n_splits)
    fold_results = []

    for train_df, test_df in folds:
        fold_model = model_factory()

        X_train = train_df[feature_cols]
        y_train = train_df[target_col]
        X_test = test_df[feature_cols]
        y_test = test_df[target_col]

        fold_model.fit(X_train, y_train)
        predictions = fold_model.predict(X_test)
        metrics = calculate_metrics(y_test, predictions)

        fold_results.append(
            EvaluationResult(metrics=metrics, predictions=predictions, actuals=y_test)
        )

    avg_metrics = {
        "mae": float(np.mean([r.metrics["mae"] for r in fold_results])),
        "rmse": float(np.mean([r.metrics["rmse"] for r in fold_results])),
        "mape": float(np.mean([r.metrics["mape"] for r in fold_results])),
    }

    return CrossValidationResult(metrics=avg_metrics, fold_results=fold_results)
