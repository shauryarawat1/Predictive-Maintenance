import pytest
from unittest.mock import patch, MagicMock
from src.data_collection import collect_metrics, run_metrics_server

@pytest.fixture

def mock_psutil():
    with patch('src.data_collection.psutil') as mock:
        mock.cpu_percent.return_value = 50.0
        mock.virtual_memory().percent = 60.0
        mock.disk_usage('/').percent = 70.0
        yield mock
        
def test_collect_metrics(mock_psutil):
    with patch('src.data_collection.CPU_USAGE.set') as mock_cpu_set, \
        patch('src.data_collection.MEMORY_USAGE.set') as mock_memory_set, \
        patch('src.data_collection.DISK_USAGE.set') as mock_disk_set:
            
        collect_metrics()
        
        mock_cpu_set.assert_called_once_with(50.0)
        mock_memory_set.assert_called_once_with(60.0)
        mock_disk_set.assert_called_once_with(70.0)
        
def test_metrics_server():
    with patch('src.data_collection.start_http_server') as mock_start_server, \
        patch('src.data_collection.collect_metrics') as mock_collect_metrics, \
        patch('src.data_collection.time.sleep', side_effect = InterruptedError):
            
        with pytest.raises(InterruptedError):
            run_metrics_server(8000)
            
        mock_start_server.assert_called_once_with(8000)
        mock_collect_metrics.assert_called_once()