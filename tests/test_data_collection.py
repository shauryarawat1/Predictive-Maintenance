import pytest
from unittest.mock import patch, MagicMock
from src.data_collection import collect_metrics, run_metrics_server
import psutil

@pytest.fixture
def mock_psutil():
    with patch('src.data_collection.psutil') as mock:
        mock.cpu_percent.return_value = 50.0
        mock.virtual_memory().percent = 60.0
        mock.disk_usage('/').percent = 70.0
        mock.net_io_counters.return_value = MagicMock(bytes_sent=1000, bytes_recv=2000)
        mock.disk_io_counters.return_value = MagicMock(read_bytes=3000, write_bytes=4000)
        yield mock

def test_collect_metrics(mock_psutil):
    with patch('src.data_collection.CPU_USAGE.set') as mock_cpu_set, \
         patch('src.data_collection.MEMORY_USAGE.set') as mock_memory_set, \
         patch('src.data_collection.DISK_USAGE.set') as mock_disk_set, \
         patch('src.data_collection.NET_IO_SENT.set') as mock_net_sent_set, \
         patch('src.data_collection.NET_IO_RECV.set') as mock_net_recv_set, \
         patch('src.data_collection.DISK_IO_READ.set') as mock_disk_read_set, \
         patch('src.data_collection.DISK_IO_WRITE.set') as mock_disk_write_set:
        
        collect_metrics()
        
        mock_cpu_set.assert_called_once_with(50.0)
        mock_memory_set.assert_called_once_with(60.0)
        mock_disk_set.assert_called_once_with(70.0)
        mock_net_sent_set.assert_called_once_with(1000)
        mock_net_recv_set.assert_called_once_with(2000)
        mock_disk_read_set.assert_called_once_with(3000)
        mock_disk_write_set.assert_called_once_with(4000)

def test_run_metrics_server():
    with patch('src.data_collection.start_http_server') as mock_start_server, \
         patch('src.data_collection.collect_metrics') as mock_collect_metrics, \
         patch('src.data_collection.time.sleep', side_effect=InterruptedError):  # To break the infinite loop
        
        with pytest.raises(InterruptedError):
            run_metrics_server(8000)
        
        mock_start_server.assert_called_once_with(8000)
        mock_collect_metrics.assert_called_once()