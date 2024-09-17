import time
import psutil
from prometheus_client import start_http_server, Gauge
from src.config import COLLECTION_INTERVAL

# Create Prometheus metrics
CPU_USAGE = Gauge('cpu_usage_percent', 'CPU usage in percent')
MEMORY_USAGE = Gauge('memory_usage_percent', 'Memory usage in percent')
DISK_USAGE = Gauge('disk_usage_percent', 'Disk usage in percent')

# Using psutil to collect the metrics from the system
def collect_metrics():
    """Collect actual system metrics"""
    
    CPU_USAGE.set(psutil.cpu_percent())
    MEMORY_USAGE.set(psutil.virtual_memory().percent)
    DISK_USAGE.set(psutil.disk_usage('/').percent)
    
# Starts Prometheus server to run metrics
def run_metrics_server(port):
    """Run the metrics collection server"""
    
    start_http_server(port)
    
    print(f"Metrics server started on port {port}")
    
    while True:
        collect_metrics()
        time.sleep(COLLECTION_INTERVAL)
        
# Allows the script to run standalone for testing

if __name__ == "__main__":
    from src.config import PROMETHEUS_PORT
    run_metrics_server(PROMETHEUS_PORT)