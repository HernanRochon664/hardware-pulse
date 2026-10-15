"""
Buy/wait models that serialize to plain JSON (no pickle).

- ``BaselineModel``: drop rate per category and "above / below the 30-day
  median" bucket, learned from the training rows. It is the bar to beat.
- ``LogisticModel``: standardized logistic regression. Training uses
  scikit-learn; prediction only needs numpy, so the dashboard and the
  scheduled job never unpickle anything.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

import numpy as np
import pandas as pd

from src.ml.dataset import CATEGORIES, FEATURES


@dataclass
class BaselineModel:
    rates: dict[str, float] = field(default_factory=dict)
    prior: float = 0.0

    @staticmethod
    def _key(category: str, above: bool) -> str:
        return f"{category}:{'above' if above else 'below'}"

    @classmethod
    def fit(cls, data: pd.DataFrame) -> BaselineModel:
        prior = float(np.nanmean(data["label"].to_numpy(dtype=float)))
        above = data["dist_med_30"] > 0
        rates: dict[str, float] = {}
        for category in CATEGORIES:
            for flag in (True, False):
                mask = (data["category"] == category) & (above == flag)
                n = int(mask.sum())
                # shrink small buckets towards the global rate
                rates[cls._key(category, flag)] = (
                    float((data.loc[mask, "label"].sum() + 10 * prior) / (n + 10)) if n else prior
                )
        return cls(rates=rates, prior=prior)

    def predict_proba(self, data: pd.DataFrame) -> np.ndarray:
        above = (data["dist_med_30"] > 0).to_numpy()
        categories = data["category"].to_numpy()
        return np.array(
            [
                self.rates.get(self._key(str(c), bool(a)), self.prior)
                for c, a in zip(categories, above, strict=True)
            ]
        )


@dataclass
class LogisticModel:
    features: list[str]
    mean: list[float]
    scale: list[float]
    coef: list[float]
    intercept: float

    @classmethod
    def fit(cls, data: pd.DataFrame, c: float = 0.05) -> LogisticModel:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        x = data[FEATURES].to_numpy(dtype=float)
        y = data["label"].to_numpy(dtype=int)
        scaler = StandardScaler().fit(x)
        mean = np.asarray(scaler.mean_, dtype=float)
        raw_scale = np.asarray(scaler.scale_, dtype=float)
        scale = np.where(raw_scale > 0, raw_scale, 1.0)
        # unweighted: the signal is read as a probability, so it must stay calibrated
        clf = LogisticRegression(C=c, max_iter=2000).fit((x - mean) / scale, y)
        return cls(
            features=list(FEATURES),
            mean=[float(v) for v in mean],
            scale=[float(v) for v in scale],
            coef=[float(v) for v in clf.coef_[0]],
            intercept=float(clf.intercept_[0]),
        )

    def _z(self, data: pd.DataFrame) -> np.ndarray:
        x = data[self.features].to_numpy(dtype=float)
        return (x - np.array(self.mean)) / np.array(self.scale)

    def predict_proba(self, data: pd.DataFrame) -> np.ndarray:
        logits = self._z(data) @ np.array(self.coef) + self.intercept
        return 1.0 / (1.0 + np.exp(-logits))

    def contributions(self, data: pd.DataFrame) -> np.ndarray:
        """Per-feature contribution to the logit (z-score times coefficient)."""
        return self._z(data) * np.array(self.coef)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict) -> LogisticModel:
        return cls(
            features=list(payload["features"]),
            mean=list(payload["mean"]),
            scale=list(payload["scale"]),
            coef=list(payload["coef"]),
            intercept=float(payload["intercept"]),
        )
