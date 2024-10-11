import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from data_processing import process_and_analyze_data
from data_collection import collect_metrics
from anomaly_detection import add_anomaly_flags

# Loading data from database
def load_data():
    data = collect_metrics()
    df = pd.DataFrame(data)
    
    df = process_and_analyze_data(df)
    
    return add_anomaly_flags(df)

# Creates subplots  for given metric
def plot_metric(df, metric):
    fig, ax = plt.subplots()
    ax.plot(df.index, df[metric], label = metric)
    
    anomalies = df[df['is_anomaly']]
    ax.scatter(anomalies.index, anomalies[metric], color = 'red', label = 'Anomalies')
    ax.set_title(f"{metric} Over Time")
    
    ax.set_xlabel('Time')
    ax.set_ylabel(metric)
    
    ax.legend()
    return fig

# Sets up streamlit dashboard that allows users to select a metric to visualize and display overview of data
def main():
    
    st.title("System Metrics Dashboard")
    
    df = load_data()
    
    st.write("## Overview")
    st.write(f"Total data points: {len(df)}")
    st.write(f"Anomalies detected: {df['is_anomaly'].sum()}")
    
    st.write("## Metrics Visualization")
    metric = st.selectbox('Select a metric to visualize', ['cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent'])
    
    fig = plot_metric(df, metric)
    st.pyplot(fig)
    
    st.write("## Raw Data")
    st.dataframe(df)
    
if __name__ == "__main__":
    main()