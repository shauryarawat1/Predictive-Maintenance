import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from src.data_processing import fetch_metrics, process_data, engineer_features

@pytest.fixture
def mock_prometheus_data():
    return {
        'cpu_usage_percent': [{'values': [['1622505600', '50'], ['1622505660', '55']]}],
        'memory_usage_percent': [{'values': [['1622505600', '60'], ['1622505660', '65']]}],
        'disk_usage_percent': [{'values': [['1622505600', '70'], ['1622505660', '75']]}],
        'network_io_sent_bytes': [{'values': [['1622505600', '1000'], ['1622505660', '1100']]}],
        'network_io_recv_bytes': [{'values': [['1622505600', '2000'], ['1622505660', '2100']]}],
        'disk_io_read_bytes': [{'values': [['1622505600', '3000'], ['1622505660', '3100']]}],
        'disk_io_write_bytes': [{'values': [['1622505600', '4000'], ['1622505660', '4100']]}]
    }

def test_fetch_metrics():
    with patch('src.data_processing.PrometheusConnect') as MockPrometheusConnect:
        mock_prom = MagicMock()
        MockPrometheusConnect.return_value = mock_prom
        mock_prom.custom_query_range.return_value = [{'values': [['1622505600', '50']]}]
        
        result = fetch_metrics('http://localhost:8000', '2021-06-01T00:00:00Z', '2021-06-01T01:00:00Z')
        
        assert len(result) == 7
        assert all(metric in result for metric in ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent',
                                                   'network_io_sent_bytes', 'network_io_recv_bytes',
                                                   'disk_io_read_bytes', 'disk_io_write_bytes'])
        assert mock_prom.custom_query_range.call_count == 7

def test_process_data(mock_prometheus_data):
    df = process_data(mock_prometheus_data)
    
    assert len(df) == 2
    assert all(col in df.columns for col in ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent',
                                             'network_io_sent_bytes', 'network_io_recv_bytes',
                                             'disk_io_read_bytes', 'disk_io_write_bytes'])
    assert df.index.name == 'timestamp'
    assert all(df[col].dtype == float for col in df.columns)

def test_engineer_features():
    dates = pd.date_range(start='2021-01-01', end='2021-01-02', freq='5min')
    df = pd.DataFrame({
        'cpu_usage_percent': np.random.rand(len(dates)) * 100,
        'memory_usage_percent': np.random.rand(len(dates)) * 100,
        'disk_usage_percent': np.random.rand(len(dates)) * 100,
        'network_io_sent_bytes': np.random.rand(len(dates)) * 1e6,
        'network_io_recv_bytes': np.random.rand(len(dates)) * 1e6,
        'disk_io_read_bytes': np.random.rand(len(dates)) * 1e6,
        'disk_io_write_bytes': np.random.rand(len(dates)) * 1e6
    }, index=dates)
    
    df_engineered = engineer_features(df)
    
    expected_columns = [
        'cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent',
        'network_io_sent_bytes', 'network_io_recv_bytes', 'disk_io_read_bytes', 'disk_io_write_bytes',
        'cpu_usage_percent_rolling_avg_5m', 'cpu_usage_percent_rolling_avg_15m',
        'memory_usage_percent_rolling_avg_5m', 'memory_usage_percent_rolling_avg_15m',
        'disk_usage_percent_rolling_avg_5m', 'disk_usage_percent_rolling_avg_15m',
        'cpu_usage_percent_rate_of_change', 'memory_usage_percent_rate_of_change', 'disk_usage_percent_rate_of_change',
        'memory_cpu_ratio', 'disk_cpu_ratio', 'hour_of_day', 'day_of_week',
        'cpu_usage_percent_lag_5m', 'cpu_usage_percent_lag_15m',
        'memory_usage_percent_lag_5m', 'memory_usage_percent_lag_15m',
        'disk_usage_percent_lag_5m', 'disk_usage_percent_lag_15m'
    ]
    
    assert all(col in df_engineered.columns for col in expected_columns)
    assert df_engineered.index.name == 'timestamp'
    assert len(df_engineered) == len(df)

def test_engineer_features_empty_df():
    empty_df = pd.DataFrame()
    df_engineered = engineer_features(empty_df)
    assert df_engineered.empty