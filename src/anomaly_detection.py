import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest # type: ignore

def detect_anomalies(df, contamination = 0.1):
    # Detect anomalies in dataframe using Isolation Forest
    
    numerical_cols = df.select_dtypes(include = [np.number]).columns
    
    # Initialize and fit isolation forest
    iso_forest = IsolationForest(contamination=contamination, random_state=42)
    anomalies = iso_forest.fit_predict(df[numerical_cols])
    
    # Convert predictions to boolean
    return pd.Series(anomalies == -1, index = df.index, name = "is_anomaly")

def add_anomaly_flags(df):
    # Add anomaly flags to dataframe
    
    df = df.copy()
    df['is_anomaly'] = detect_anomalies(df)
    return df