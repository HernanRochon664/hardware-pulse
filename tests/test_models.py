import pytest
import pandas as pd
import numpy as np
from src.models.naive import NaivePersistenceModel
from src.models.evaluation import calculate_metrics, split_temporal, evaluate_model_performance

def test_calculate_metrics():
    # Simple case: y_true = [100, 200], y_pred = [110, 190]
    # MAE = (10 + 10) / 2 = 10
    # RMSE = sqrt((10^2 + 10^2) / 2) = sqrt(100) = 10
    # MAPE = ((10/100 + 10/200) / 2) * 100 = (0.1 + 0.05) / 2 * 100 = 7.5%
    y_true = pd.Series([100, 200])
    y_pred = pd.Series([110, 190])
    
    metrics = calculate_metrics(y_true, y_pred)
    
    assert metrics["mae"] == pytest.approx(10.0)
    assert metrics["rmse"] == pytest.approx(10.0)
    assert metrics["mape"] == pytest.approx(7.5)

def test_split_temporal():
    df = pd.DataFrame({"a": range(10)})
    
    # Valid split
    train, test = split_temporal(df, 0.8)
    assert len(train) == 8
    assert len(test) == 2
    assert train.index.max() < test.index.min()
    
    # Invalid ratio
    with pytest.raises(ValueError, match="split_ratio must be between 0 and 1"):
        split_temporal(df, 1.5)
    with pytest.raises(ValueError, match="split_ratio must be between 0 and 1"):
        split_temporal(df, -0.1)

def test_naive_persistence_model_missing_column():
    model = NaivePersistenceModel(price_column="non_existent")
    X = pd.DataFrame({"precio_lag_1": [100, 110]})
    
    with pytest.raises(ValueError, match="Feature 'non_existent' not found in input DataFrame"):
        model.predict(X)

def test_evaluate_model_performance_integration():
    # Setup minimal data
    df = pd.DataFrame({
        "precio_lag_1": [10, 11, 12, 13, 14],
        "target": [11, 12, 13, 14, 15]
    })
    model = NaivePersistenceModel()
    
    # split_ratio=0.6 -> train: 3 samples, test: 2 samples
    results = evaluate_model_performance(
        model=model,
        df=df,
        target_col="target",
        feature_cols=["precio_lag_1"],
        split_ratio=0.6
    )
    
    assert results.metrics is not None
    assert results.predictions is not None
    assert len(results.predictions) == 2
    # Naive persistence should predict exactly the values in precio_lag_1
    # Test set is rows [3, 4] -> precio_lag_1 values are [13, 14]
    assert results.predictions.iloc[0] == 13
    assert results.predictions.iloc[1] == 14
