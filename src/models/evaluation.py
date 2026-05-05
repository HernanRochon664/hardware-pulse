"""
Evaluation module for price prediction models.

This module provides tools to split time-series data temporally, calculate
regression metrics (MAE, RMSE, MAPE), and execute a full evaluation
pipeline using a hold-out temporal validation strategy.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
from .base import PriceModel

@dataclass
class EvaluationResult:
    """
    Container for model evaluation results.
    
    Attributes:
        metrics: Dictionary containing MAE, RMSE, and MAPE.
        predictions: Series of predicted prices.
        actuals: Series of actual observed prices.
    """
    metrics: dict[str, float]
    predictions: pd.Series
    actuals: pd.Series

def calculate_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
    """
    Calculate regression metrics for price prediction.
    
    Includes Mean Absolute Error (MAE), Root Mean Squared Error (RMSE), 
    and Mean Absolute Percentage Error (MAPE) as the primary business metric.
    
    Args:
        y_true: Ground truth target values.
        y_pred: Estimated target values.
        
    Returns:
        Dictionary with keys 'mae', 'rmse', and 'mape'.
    """
    # Avoid division by zero in MAPE
    mask = y_true != 0
    mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(root_mean_squared_error(y_true, y_pred)),
        "mape": float(mape)
    }

def split_temporal(df: pd.DataFrame, split_ratio: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Divide the dataframe into train and test sets preserving the chronological order.
    
    This prevents data leakage by ensuring that the training set only contains 
    data from the past relative to the test set.
    
    Args:
        df: Input DataFrame sorted by date.
        split_ratio: Proportion of data to use for training (0 < split_ratio < 1).
        
    Returns:
        A tuple containing (train_df, test_df).
        
    Raises:
        ValueError: If split_ratio is not between 0 and 1.
    """
    if not 0 < split_ratio < 1:
        raise ValueError("split_ratio must be between 0 and 1")
        
    split_idx = int(len(df) * split_ratio)
    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]
    
    return train_df, test_df

def evaluate_model_performance(
    model: PriceModel, 
    df: pd.DataFrame, 
    target_col: str, 
    feature_cols: list[str], 
    split_ratio: float = 0.8
) -> EvaluationResult:
    """
    Complete evaluation pipeline: temporal split -> fit -> predict -> metrics.
    
    This function implements a hold-out temporal validation strategy to assess 
    how the model performs on unseen future data.
    
    Args:
        model: The model instance implementing PriceModel protocol.
        df: DataFrame containing features and target, sorted chronologically.
        target_col: Name of the target price column.
        feature_cols: List of columns to be used as features for X.
        split_ratio: Proportion of data for training.
        
    Returns:
        EvaluationResult containing metrics, predictions, and ground truth.
    """
    # 1. Temporal Split
    train_df, test_df = split_temporal(df, split_ratio)
    
    # 2. Separate X and y
    X_train = train_df[feature_cols]
    y_train = train_df[target_col]
    
    X_test = test_df[feature_cols]
    y_test = test_df[target_col]
    
    # 3. Fit model
    model.fit(X_train, y_train)
    
    # 4. Predict
    predictions = model.predict(X_test)
    
    # 5. Metrics
    metrics = calculate_metrics(y_test, predictions)
    
    return EvaluationResult(
        metrics=metrics,
        predictions=predictions,
        actuals=y_test
    )
