import pytest
import pandas as pd
import numpy as np
from src.anomaly_detection import detect_anomalies, add_anomaly_flags

@pytest.fixture
def sample_df():
    np.random.seed(42)
    
    dates = pd.date_range(start = '2021-01-01', end = '2021-01-02', freq = "5min")
    
    df = pd.DataFrame({
        'cpu_usage_percent': np.random.rand(len(dates)) * 100,
        'memory_usage_percent': np.random.rand(len(dates)) * 100,
        'disk_usage_percent': np.random.rand(len(dates)) * 100
    }, index = dates)
    
    return df

def test_detect_anomalies(sample_df):
    anomalies = detect_anomalies(sample_df)
    assert isinstance(anomalies, pd.Series)
    assert anomalies.dtype == bool
    assert len(anomalies) == len(sample_df)
    assert anomalies.sum() > 0
    
def test_add_anomaly_flags(sample_df):
    df_with_anomalies = add_anomaly_flags(sample_df)
    
    assert 'is_anomaly' in df_with_anomalies.columns
    
    assert df_with_anomalies['is_anomaly'].dtype == bool
    assert df_with_anomalies['is_anomaly'].sum() > 0