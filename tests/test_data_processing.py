import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.data_processing import fetch_metrics, process_data, analyze_data

@pytest.fixture
def mock_prometheus_data():
    return {
        'cpu_usage_percent': [{'values': [['1622505600', '50'], ['1622505660', '55']]}],
        'memory_usage_percent': [{'values': [['1622505600', '60'], ['1622505660', '65']]}],
        'disk_usage_percent': [{'values': [['1622505600', '70'], ['1622505660', '75']]}]
    }

def test_fetch_metrics():
    with patch('src.data_processing.PrometheusConnect') as MockPrometheusConnect:
        mock_prom = MagicMock()
        MockPrometheusConnect.return_value = mock_prom
        mock_prom.custom_query_range.return_value = [{'values': [['1622505600', '50']]}]
        
        result = fetch_metrics('http://localhost:8000', '2021-06-01T00:00:00Z', '2021-06-01T01:00:00Z')
        
        assert len(result) == 3
        assert all(metric in result for metric in ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent'])
        mock_prom.custom_query_range.assert_called()

def test_process_data(mock_prometheus_data):
    df = process_data(mock_prometheus_data)
    
    assert len(df) == 2
    assert all(col in df.columns for col in ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent'])
    assert df.index.name == 'timestamp'
    assert df['cpu_usage_percent'].dtype == float
    assert df['memory_usage_percent'].dtype == float
    assert df['disk_usage_percent'].dtype == float

def test_analyze_data():
    df = pd.DataFrame({
        'cpu_usage_percent': [50.0, 60.0, 70.0],
        'memory_usage_percent': [55.0, 65.0, 75.0],
        'disk_usage_percent': [60.0, 70.0, 80.0]
    }, index=pd.date_range('2021-06-01', periods=3, freq='min'))
    
    analysis = analyze_data(df)
    
    assert 'cpu_usage_percent_mean' in analysis
    assert 'memory_usage_percent_mean' in analysis
    assert 'disk_usage_percent_mean' in analysis
    assert 'cpu_usage_percent_max' in analysis
    assert 'memory_usage_percent_max' in analysis
    assert 'disk_usage_percent_max' in analysis
    assert analysis['cpu_usage_percent_mean'] == 60.0
    assert analysis['cpu_usage_percent_max'] == 70.0

def test_analyze_data_empty():
    df = pd.DataFrame()
    analysis = analyze_data(df)
    assert analysis == "No data available for analysis."
