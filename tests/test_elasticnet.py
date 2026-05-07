"""
Tests for the ElasticNetPriceModel.
Ensures that fitting, prediction, persistence, and feature importance
extraction work as expected.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.exceptions import NotFittedError
from src.models.elasticnet import ElasticNetPriceModel

def test_predict_before_fit():
    """Verify that calling predict() before fit() raises NotFittedError."""
    model = ElasticNetPriceModel()
    X = pd.DataFrame({"f1": [1, 2], "f2": [3, 4]})
    with pytest.raises(NotFittedError):
        model.predict(X)

def test_predict_column_mismatch():
    """Verify that predict() raises ValueError when the input features don't match the training set."""
    model = ElasticNetPriceModel()
    X_train = pd.DataFrame({"f1": [1, 2], "f2": [3, 4]})
    y_train = pd.Series([10, 20])
    model.fit(X_train, y_train)
    
    # Missing column
    X_test_missing = pd.DataFrame({"f1": [1, 2]})
    with pytest.raises(ValueError, match="Feature mismatch"):
        model.predict(X_test_missing)
        
    # Extra column
    X_test_extra = pd.DataFrame({"f1": [1, 2], "f2": [3, 4], "f3": [5, 6]})
    with pytest.raises(ValueError, match="Feature mismatch"):
        model.predict(X_test_extra)

def test_full_flow_persistence(tmp_path):
    """Verify that the model can be saved and loaded without losing predict performance."""
    model = ElasticNetPriceModel()
    X_train = pd.DataFrame({"f1": np.random.rand(10), "f2": np.random.rand(10)})
    y_train = pd.Series(np.random.rand(10))
    
    model.fit(X_train, y_train)
    preds_before = model.predict(X_train)
    
    path = tmp_path / "model.joblib"
    model.save(path)
    
    loaded_model = ElasticNetPriceModel.load(path)
    preds_after = loaded_model.predict(X_train)
    
    pd.testing.assert_series_equal(preds_before, preds_after)

def test_feature_importances():
    """Verify that feature_importances() returns a correctly indexed Series of coefficients."""
    model = ElasticNetPriceModel()
    X_train = pd.DataFrame({
        "f1": [1, 2, 3, 4, 5],
        "f2": [10, 20, 30, 40, 50]
    })
    y_train = pd.Series([2, 4, 6, 8, 10]) # y = 2 * f1 (approx)
    
    model.fit(X_train, y_train)
    importances = model.feature_importances()
    
    assert isinstance(importances, pd.Series)
    assert list(importances.index) == ["f1", "f2"] or list(importances.index) == ["f2", "f1"]
    assert len(importances) == 2
