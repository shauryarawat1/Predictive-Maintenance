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

def process_data(metrics_data):
    """Process and combine metrics"""
    
    dfs = []
    
    for metric, data in metrics_data.items():
        if data and data[0]['values']:
            df = pd.DataFrame(data[0]['values'], columns = ['timestamp', metric])
            try:
                df[metric] = pd.to_numeric(df[metric], errors='coerce')
                
            except ValueError as e:
                print(f"Error converting {metric} to numeric: {e}")
                print(f"Problematic data: {df[metric].head()}")
                continue
            
            dfs.append(df)
            
        if not dfs:
            return pd.DataFrame()
        
        df = dfs[0]
        
        for other_df in dfs[1:]:
            df = df.merge(other_df, on='timestamp', how='outer')
            
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('timestamp', inplace=True)
        
        return df
    
