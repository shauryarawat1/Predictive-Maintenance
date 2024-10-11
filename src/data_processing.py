# src/data_processing.py

import pandas as pd
import numpy as np

def process_data(df):
    """
    Process the input dataframe.
    
    Args:
    df (pd.DataFrame): Input dataframe with raw metrics
    
    Returns:
    pd.DataFrame: Processed dataframe
    """
    # Ensure the dataframe is not empty
    if df.empty:
        return pd.DataFrame()
    
    # Perform any necessary processing here
    df = df.copy()
    
    # Remove any rows with negative values (assuming these are invalid)
    df = df[(df >= 0).all(axis=1)]
    
    # Cap values at 100 (assuming these are percentages)
    df = df.clip(upper=100)
    
    # Handle NaN values
    df = df.fillna(df.mean())
    
    return df

def engineer_features(df):
    """
    Engineer additional features from the processed data.
    
    Args:
    df (pd.DataFrame): Processed dataframe
    
    Returns:
    pd.DataFrame: Dataframe with engineered features
    """
    df = df.copy()
    
    # Add rolling averages
    for column in df.columns:
        df[f'{column}_rolling_avg_5min'] = df[column].rolling(window='5min').mean()
        df[f'{column}_rolling_avg_1hour'] = df[column].rolling(window='1H').mean()
    
    # Add rate of change (percentage change)
    for column in df.columns:
        df[f'{column}_rate_of_change'] = df[column].pct_change()
    
    # Add time-based features
    df['hour_of_day'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    
    # Add lag features
    for column in df.columns:
        df[f'{column}_lag_5min'] = df[column].shift(periods=5)
        df[f'{column}_lag_1hour'] = df[column].shift(periods=60)
    
    return df

def calculate_statistics(df):
    """
    Calculate various statistics for the dataframe.
    
    Args:
    df (pd.DataFrame): Input dataframe
    
    Returns:
    dict: Dictionary of calculated statistics
    """
    stats = {}
    for column in df.columns:
        stats[f'{column}_mean'] = df[column].mean()
        stats[f'{column}_median'] = df[column].median()
        stats[f'{column}_std'] = df[column].std()
        stats[f'{column}_min'] = df[column].min()
        stats[f'{column}_max'] = df[column].max()
    
    return stats

def process_and_analyze_data(df):
    """
    Process, engineer features, and analyze the data.

    Args:
    df (pd.DataFrame): Raw input dataframe

    Returns:
    tuple: (Processed dataframe with engineered features, Dictionary of statistics)
    """
    df_processed = process_data(df)
    df_engineered = engineer_features(df_processed)
    stats = calculate_statistics(df_engineered)
    
    return df_engineered, stats