# tests/test_dashboard.py

import pytest
import pandas as pd
import numpy as np
from src.dashboard import load_data, plot_metric, generate_sample_data

def test_generate_sample_data():
    df = generate_sample_data()
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {'cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent'}
    assert len(df) == 100  # default number of samples

def test_load_data():
    df, stats = load_data()
    assert isinstance(df, pd.DataFrame)
    assert isinstance(stats, dict)
    assert 'is_anomaly' in df.columns
    assert set(df.columns) >= {'cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent', 'is_anomaly'}
    assert df['is_anomaly'].dtype == bool
    assert not df['is_anomaly'].isna().any()  # Ensure no NaN values in is_anomaly column
    
    # Check if stats contains expected keys
    expected_stat_keys = ['cpu_usage_percent_mean', 'memory_usage_percent_mean', 'disk_usage_percent_mean']
    assert all(key in stats for key in expected_stat_keys)

@pytest.fixture
def sample_data():
    df = generate_sample_data(5)
    df['is_anomaly'] = [False, False, True, False, True]
    return df

def test_plot_metric(sample_data):
    fig = plot_metric(sample_data, 'cpu_usage_percent')
    assert fig is not None
    # You might want to add more specific checks about the figure content