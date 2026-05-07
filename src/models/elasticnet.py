"""
ElasticNet price forecasting model for hardware-pulse.

Implements a regularized linear regression model using sklearn's ElasticNet
wrapped in a Pipeline with StandardScaler preprocessing. This is the primary
forecasting model, intended to be compared against NaivePersistenceModel.

Design decisions:
- Uses sklearn Pipeline (scaler + model) to guarantee identical preprocessing
  at training and inference time. Persisting the full pipeline via joblib
  ensures there is no train/inference skew.
- feature_columns_ is stored after fit() as a defensive check against
  inference being called with a different feature set than training.
- is_fitted state is delegated to sklearn's check_is_fitted() — no manual flag.
- Retraining cadence is controlled by the calling pipeline/orchestrator,
  not by this class. This class only does fit/predict.

Persistence:
- Trained pipelines are saved to artifacts/ via joblib (recommended for
  sklearn models — handles NumPy arrays efficiently).
- load() and save() are class/instance methods for convenience, but the
  decision of when to call them belongs to the orchestrator.

Retraining policy (enforced externally):
- Monthly retraining is the default assumption for the MVP.
- Daily inference loads the last persisted artifact from artifacts/.
"""

from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils.validation import check_is_fitted


class ElasticNetPriceModel:
    """
    ElasticNet-based price forecasting model.

    Wraps a sklearn Pipeline (StandardScaler → ElasticNet) and implements
    the PriceModel protocol, making it interchangeable with any other model
    in evaluate_model_performance().

    Unlike NaivePersistenceModel, this model is stateful: fit() learns
    coefficients from historical data that are then used in predict().

    Args:
        alpha:        Regularization strength. Higher values → more regularization.
        l1_ratio:     Mix between L1 (Lasso) and L2 (Ridge) penalties.
                      0.0 = pure Ridge, 1.0 = pure Lasso, 0.5 = ElasticNet default.
        max_iter:     Maximum iterations for the coordinate descent solver.
        random_state: Random seed for reproducibility.

    Example:
        model = ElasticNetPriceModel(alpha=0.5, l1_ratio=0.3)
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        model.save(Path("artifacts/model.joblib"))

        # Later, in the daily inference pipeline:
        model = ElasticNetPriceModel.load(Path("artifacts/model.joblib"))
        predictions = model.predict(X_new)
    """

    def __init__(
        self,
        alpha: float = 1.0,
        l1_ratio: float = 0.5,
        max_iter: int = 10_000,
        random_state: int = 42,
    ) -> None:
        self.alpha = alpha
        self.l1_ratio = l1_ratio
        self.max_iter = max_iter
        self.random_state = random_state

        self.pipeline = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                (
                    "model",
                    ElasticNet(
                        alpha=self.alpha,
                        l1_ratio=self.l1_ratio,
                        max_iter=self.max_iter,
                        random_state=self.random_state,
                    ),
                ),
            ]
        )

        # Set after fit() — used to validate feature consistency at inference
        self.feature_columns_: list[str] | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Train the ElasticNet pipeline on historical price data.

        Fits the StandardScaler and ElasticNet on X and y, then stores
        the feature column names to enable defensive validation in predict().

        Args:
            X: Feature DataFrame. Expected columns include lag features
               (precio_lag_1, precio_lag_2), rolling median, dispersion,
               and FX rate — as produced by feature_snapshots.
            y: Target Series — weekly median price in USD per SKU.
        """
        self.pipeline.fit(X, y)
        self.feature_columns_ = list(X.columns)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Generate price predictions for the given feature set.

        Validates that the model has been fitted and that X contains
        the same features used during training.

        Args:
            X: Feature DataFrame with the same schema as used in fit().

        Returns:
            Series of predicted prices (USD), aligned with X's index.

        Raises:
            sklearn.exceptions.NotFittedError: If called before fit().
            ValueError: If X contains different columns than training data.
        """
        check_is_fitted(self.pipeline)

        if self.feature_columns_ is not None:
            missing = set(self.feature_columns_) - set(X.columns)
            extra = set(X.columns) - set(self.feature_columns_)
            if missing or extra:
                raise ValueError(
                    f"Feature mismatch between training and inference.\n"
                    f"  Missing columns: {sorted(missing)}\n"
                    f"  Unexpected columns: {sorted(extra)}"
                )
            X = X[self.feature_columns_]

        predictions = self.pipeline.predict(X)
        return pd.Series(predictions, index=X.index, name="predicted_price_usd")

    def save(self, path: Path) -> None:
        """
        Persist the trained model pipeline to disk using joblib.

        Saves the full ElasticNetPriceModel instance, including the fitted
        sklearn Pipeline and stored feature_columns_. This guarantees that
        the scaler fitted on training data is used unchanged at inference time.

        Args:
            path: Destination path (e.g. Path("artifacts/model.joblib")).
                  Parent directory is created if it does not exist.

        Raises:
            sklearn.exceptions.NotFittedError: If called before fit().
        """
        check_is_fitted(self.pipeline)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self, path)

    @classmethod
    def load(cls, path: Path) -> "ElasticNetPriceModel":
        """
        Load a persisted ElasticNetPriceModel from disk.

        Args:
            path: Path to a .joblib file previously saved via save().

        Returns:
            A fitted ElasticNetPriceModel instance ready for inference.

        Raises:
            FileNotFoundError: If path does not exist.
        """
        if not path.exists():
            raise FileNotFoundError(
                f"No model artifact found at '{path}'. "
                "Run the retraining pipeline before inference."
            )
        return joblib.load(path)

    def feature_importances(self) -> pd.Series:
        """
        Return ElasticNet coefficients as a ranked Series.

        Useful for SHAP-style interpretation and for understanding
        which features drive price predictions.

        Returns:
            Series of coefficients indexed by feature name,
            sorted by absolute value descending.

        Raises:
            sklearn.exceptions.NotFittedError: If called before fit().
        """
        check_is_fitted(self.pipeline)

        elasticnet: ElasticNet = self.pipeline.named_steps["model"]
        coefs = pd.Series(
            elasticnet.coef_,
            index=self.feature_columns_,
            name="coefficient",
        )
        return coefs.reindex(coefs.abs().sort_values(ascending=False).index)