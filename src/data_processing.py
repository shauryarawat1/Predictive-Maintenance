import pandas as pd
import numpy as np

def process_data(metrics_data):
    """Process and combine metrics data"""
    dfs = []
    for metric, data in metrics_data.items():
        if data and data[0]['values']:
            df = pd.DataFrame(data[0]['values'], columns=['timestamp', metric])
            df[metric] = pd.to_numeric(df[metric], errors='coerce')
            dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.merge(other_df, on='timestamp', how='outer')
    
    # Convert timestamp to datetime, handling both string and numeric formats
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df.set_index('timestamp', inplace=True)
    df = df.sort_index()  # Ensure the index is sorted
    
    # Ensure all metrics are present, even if they're all NaN
    for metric in metrics_data.keys():
        if metric not in df.columns:
            df[metric] = np.nan
    
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