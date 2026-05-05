"""
Naive persistence model for hardware-pulse price forecasting.

This is the baseline model against which all other models are compared.
It implements the simplest possible forecasting strategy: predict that
next week's price equals this week's price (persistence / random walk).

Any model that cannot consistently outperform this baseline on MAPE
is not adding value over the naive assumption.
"""

from __future__ import annotations

import pandas as pd


class NaivePersistenceModel:
    """
    Baseline price model using the persistence (random walk) strategy.

    Predicts next period's price as the most recently observed price.
    This is a stateless model — fit() is a no-op by design, not an
    incomplete implementation. The persistence model has no parameters
    to learn; it simply returns the last observed value as its forecast.

    The expected feature column is 'precio_lag_1', which in feature_snapshots
    represents the weekly median price of the previous period — exactly the
    "last observed price" this model needs.

    Example:
        model = NaivePersistenceModel()
        model.fit(X_train, y_train)  # no-op
        predictions = model.predict(X_test)
    """

    def __init__(self, price_column: str = "precio_lag_1") -> None:
        """
        Args:
            price_column: Name of the feature column containing the last
                          observed price. Defaults to 'precio_lag_1', which
                          corresponds to the previous week's median price
                          in feature_snapshots.
        """
        self.price_column = price_column

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        No-op. Persistence model is stateless and requires no training.

        Implemented to satisfy the PriceModel protocol and to allow
        NaivePersistenceModel to be used interchangeably with trained models
        in evaluation pipelines.

        Args:
            X: Feature DataFrame (unused).
            y: Target Series (unused).
        """

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Predict next period's price as the last observed price.

        Args:
            X: Feature DataFrame containing the price_column.

        Returns:
            Series of predicted prices with the same index as X.

        Raises:
            ValueError: If price_column is not present in X.
        """
        if self.price_column not in X.columns:
            raise ValueError(
                f"Feature '{self.price_column}' not found in input DataFrame. "
                f"Available columns: {list(X.columns)}"
            )
        return X[self.price_column].copy()