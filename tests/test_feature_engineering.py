import pytest
import pandas as pd
import numpy as np
from src.data_processing import engineer_features

@pytest.fixture
def sample_df():
    dates = pd.date_range(start = '2021-01-01', end = '2021-01-02', freq = '5min')
    
    df = pd.DataFrame({
        'cpu_usage_percent': np.random.rand(len(dates)) * 100,
        'memory_usage_percent': np.random.rand(len(dates)) * 100,
        'disk_usage_percent': np.random.rand(len(dates)) * 100,
        'network_io_sent_bytes': np.random.rand(len(dates)) * 1e6,
        'network_io_recv_bytes': np.random.rand(len(dates)) * 1e6,
        'disk_io_read_bytes': np.random.rand(len(dates)) * 1e6,
        'disk_io_write_bytes': np.random.rand(len(dates)) * 1e6
    }, index = dates)
    
    return df

def test_engineer_features(sample_df):
    df_engineered = engineer_features(sample_df)
    
    # Check that new features are present
    assert 'cpu_usage_percent_rolling_avg_5m' in df_engineered.columns
    assert 'memory_usage_percent_rate_of_change' in df_engineered.columns
    assert 'memory_cpu_ratio' in df_engineered.columns
    assert 'hour_of_day' in df_engineered.columns
    assert 'cpu_usage_percent_lag_5m' in df_engineered.columns
    
    # check if time based features are correct
    assert (df_engineered['hour_of_day'] == df_engineered.index.hour).all()
    assert (df_engineered['day_of_week'] == df_engineered.index.dayofweek).all()
    
    # Check that ratios are calculated correctly
    np.testing.assert_almost_equal(
        df_engineered['memory_cpu_ratio'],
        sample_df['memory_usage_percent'] / sample_df['cpu_usage_percent']
    )
    
    # Check that rolling averages are calculated correctly
    np.testing.assert_almost_equal(
        df_engineered['cpu_usage_percent_rolling_avg_5m'],
        sample_df['cpu_usage_percent'].rolling(window = '5T').mean()
    )
    
    pd.testing.assert_series_equal(
        df_engineered['memory_cpu_ratio'],
        sample_df['memory_usage_percent'] / sample_df['cpu_usage_percent'],
        check_names = False
    )
    
    pd.testing.assert_series_equal(
        df_engineered['cpu_usage_percent_rolling_avg_5m'],
        sample_df['cpu_usage_percent'].rolling(window='5min').mean(),
        check_names=False
    )
    
def test_engineer_features_empty_df():
    empty_df = pd.DataFrame()
    df_engineered = engineer_features(empty_df)
    assert df_engineered.empty