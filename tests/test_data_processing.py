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