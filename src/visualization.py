import matplotlib.pyplot as plt

def plot_metrics_with_anomalies(df, metric_col, title):
    
    # Plots metric over time and highlights anomalies
    
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df[metric_col], label = metric_col)
    
    anomalies = df[df['is_anomaly']]
    
    plt.scatter(anomalies.index, anomalies[metric_col], color = 'red', label = 'Anomalies')
    
    plt.title(title)
    plt.xlabel("time")
    plt.ylabel(metric_col)
    
    plt.legend()
    plt.tight_layout()
    plt.show()