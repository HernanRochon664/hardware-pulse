"""
Base interface for hardware-pulse price models.

Defines the PriceModel Protocol — the contract that all price prediction
models must satisfy. Using Protocol (structural subtyping) instead of ABC
allows external models (e.g. sklearn estimators) to satisfy the interface
without explicit inheritance.

Any class that implements fit(X, y) and predict(X) with compatible
signatures is a valid PriceModel.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class PriceModel(Protocol):
    """
    Structural interface for price prediction models.

    All models in src/models/ implement this protocol, enabling
    evaluate_model_performance() to be model-agnostic.

    Conventions:
    - X: DataFrame of features (one row per prediction unit)
    - y: Series of target prices (price_usd, weekly median per SKU)
    - predict() returns a Series aligned with X's index
    """

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Train the model using historical features and target prices.

        Args:
            X: Feature DataFrame (week_start, canonical_product_id, lag features, etc.)
            y: Target Series — weekly median price in USD per SKU.
        """
        ...

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Generate price predictions for the given feature set.

        Args:
            X: Feature DataFrame with the same schema as used in fit().

        Returns:
            Series of predicted prices, aligned with X's index.
        """
        ...