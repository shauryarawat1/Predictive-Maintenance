# src/anomaly_detection.py

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer

def detect_anomalies(df, contamination=0.1):
    """
    Detect anomalies in the given dataframe using Isolation Forest.
    
    Args:
    df (pd.DataFrame): Input dataframe with features
    contamination (float): The proportion of outliers in the data set

    Returns:
    np.ndarray: Boolean array where True indicates an anomaly
    """
    # Select numerical columns
    numerical_cols = df.select_dtypes(include=[np.number]).columns

    # Create an imputer to handle NaN values
    imputer = SimpleImputer(strategy='mean')

    # Fit and transform the data
    X = imputer.fit_transform(df[numerical_cols])

    # Initialize and fit the Isolation Forest
    iso_forest = IsolationForest(contamination=contamination, random_state=42)
    anomalies = iso_forest.fit_predict(X)

    # Convert predictions to boolean (True if anomaly)
    return anomalies == -1

def add_anomaly_flags(df):
    """
    Add anomaly flags to the dataframe.

    Args:
    df (pd.DataFrame): Input dataframe with features

    Returns:
    pd.DataFrame: Dataframe with added anomaly flags
    """
    df = df.copy()
    df['is_anomaly'] = detect_anomalies(df)
    return df