import pytest
import pandas as pd
from src.dashboard import load_data, plot_metric

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'cpu_usage_percent': [50, 60, 70, 80, 90],
        'memory_usage_percent': [60, 65, 70, 75, 80],
        'disk_usage_percent': [70, 72, 74, 76, 78],
        'is_anomaly': [False, False, True, False, True]
    }, index=pd.date_range(start='2023-01-01', periods=5, freq='H'))
    
def test_load_data():
    df = load_data()
    assert isinstance(df, pd.DataFrame)
    assert 'is_anomaly' in df.columns
    assert set(df.columns) >= {'cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent', 'is_anomaly'}
    
def test_plot_metric(sample_data):
    fig = plot_metric(sample_data, 'cpu_usage_percent')
    assert fig is not None