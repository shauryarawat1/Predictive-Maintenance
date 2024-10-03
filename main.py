import time
from datetime import datetime, timedelta
import multiprocessing
from src.data_collection import run_metrics_server
from src.data_processing import fetch_metrics, process_data, analyze_data, engineer_features
from src.data_storage import store_metrics
from src.config import PROMETHEUS_URL, PROMETHEUS_PORT, COLLECTION_INTERVAL

def main():
    print("Starting metrics collection server...")
    
    # Run metrics server in a separate process
    server_process = multiprocessing.Process(target= run_metrics_server, args= (PROMETHEUS_PORT,))
    server_process.start()
    
    # Wait for server to start
    time.sleep(5)
    
    print("Collecting and storing data for 1 minute...")
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes = 1)
    
    # Loop to collect data for 1 minute
    while datetime.now() < end_time:
        current_time = datetime.now()
        
        metrics_data = fetch_metrics(PROMETHEUS_URL, current_time - timedelta(seconds = 10), current_time)
        df = process_data(metrics_data)
        
        df_engineered = engineer_features(df)
        
        # Save process data to database
        store_metrics(df)
        time.sleep(10)
        
    print("\nData Collection and storage completed")
    
    analysis_results = analyze_data(df_engineered)
    
    print("\nLast minute data analysis:")
    
    for key, value in analysis_results.items():
        print(f"{key}: {value:.2f}")
        
    server_process.terminate()
    server_process.join()
    
if __name__ == "__main__":
    main()
 