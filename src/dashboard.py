import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np
from src.data_processing import process_and_analyze_data
from src.anomaly_detection import add_anomaly_flags

def generate_sample_data(n_samples=100):
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=n_samples)
    date_range = pd.date_range(start=start_time, end=end_time, periods=n_samples)
    
    data = {
        'cpu_usage_percent': np.random.uniform(0, 100, n_samples),
        'memory_usage_percent': np.random.uniform(0, 100, n_samples),
        'disk_usage_percent': np.random.uniform(0, 100, n_samples),
    }
    return pd.DataFrame(data, index=date_range)

def load_data():
    # In a real scenario, this would load data from your database
    # For now, we'll generate some sample data
    df = generate_sample_data()
    df, stats = process_and_analyze_data(df)
    df = add_anomaly_flags(df)
    return df, stats

def plot_metric(df, metric):
    fig, ax = plt.subplots()
    ax.plot(df.index, df[metric], label=metric)
    anomalies = df[df['is_anomaly'] == True]
    ax.scatter(anomalies.index, anomalies[metric], color='red', label='Anomalies')
    ax.set_title(f'{metric} Over Time')
    ax.set_xlabel('Time')
    ax.set_ylabel(metric)
    ax.legend()
    return fig

def main():
    st.title('System Metrics Dashboard')

    df = load_data()

    st.write('## Overview')
    st.write(f'Total data points: {len(df)}')
    st.write(f'Anomalies detected: {df["is_anomaly"].sum()}')

    st.write('## Metrics Visualization')
    metric = st.selectbox('Select a metric to visualize', ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent'])
    
    fig = plot_metric(df, metric)
    st.pyplot(fig)

    st.write('## Raw Data')
    st.dataframe(df)

if __name__ == '__main__':
    main()