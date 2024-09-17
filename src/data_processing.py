import pandas as pd
from prometheus_api_client import PrometheusConnect

# Fetch metrics from Prometheus
def fetch_metrics(prom_url, start_time, end_time, step = '1m'):
    prom = PrometheusConnect(url = prom_url, disable_ssl = True)
    
    metrics = ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent']
    
    data = {}
    
    for metric in metrics:
        data[metric] = prom.custom_query_range(
            query= metric,
            start_time= start_time,
            end_time= end_time,
            step= step
        )
        
    return data

# Process and combine metrics data