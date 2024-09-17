import time
import psutil
from prometheus_client import start_http_server, Gauge
from src.config import COLLECTION_INTERVAL

# Create Prometheus metrics
CPU_USAGE = Gauge('cpu_usage_percent', 'CPU usage in percent')
MEMORY_USAGE = Gauge('memory_usage_percent', 'Memory usage in percent')
DISK_USAGE = Gauge('disk_usage_percent', 'Disk usage in percent')

# Using psutil to collect the metrics from the system
