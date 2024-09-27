import pandas as pd
import numpy as np
from prometheus_api_client import PrometheusConnect

def fetch_metrics(prom_url, start_time, end_time, step='1m'):
    """Fetch metrics from Prometheus"""
    prom = PrometheusConnect(url=prom_url, disable_ssl=True)
    
    metrics = ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent',
               'network_io_sent_bytes', 'network_io_recv_bytes', 'disk_io_read_bytes', 'disk_io_write_bytes']
    data = {}
    
    for metric in metrics:
        data[metric] = prom.custom_query_range(
            query=metric,
            start_time=start_time,
            end_time=end_time,
            step=step
        )
    
    return data

def process_data(metrics_data):
    """Process and combine metrics data"""
    dfs = []
    for metric, data in metrics_data.items():
        if data and data[0]['values']:
            df = pd.DataFrame(data[0]['values'], columns=['timestamp', metric])
            df[metric] = pd.to_numeric(df[metric], errors='coerce').astype(float)  # Explicitly convert to float
            dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.merge(other_df, on='timestamp', how='outer')
    
    # Convert timestamp to datetime, handling both string and numeric formats
    df['timestamp'] = pd.to_datetime(pd.to_numeric(df['timestamp'], errors='coerce'), unit='s', errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df.set_index('timestamp', inplace=True)
    df = df.sort_index()  # Ensure the index is sorted
    
    # Calculate rate of change for I/O metrics
    for col in ['network_io_sent_bytes', 'network_io_recv_bytes', 'disk_io_read_bytes', 'disk_io_write_bytes']:
        if col in df.columns:
            df[f'{col}_rate'] = df[col].diff() / df.index.to_series().diff().dt.total_seconds()
    
    return df

def analyze_data(df):
    """Perform basic analysis on the data"""
    if df.empty:
        return "No data available for analysis."
    
    analysis = {}
    for column in df.columns:
        analysis[f'{column}_mean'] = df[column].mean()
        analysis[f'{column}_max'] = df[column].max()
    
    return analysis