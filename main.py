import time
from datetime import datetime, timedelta
import multiprocessing
from src.data_collection import run_metrics_server
from src.data_processing import fetch_metrics, process_data, analyze_data
from src.config import PROMETHEUS_URL, PROMETHEUS_PORT, COLLECTION_INTERVAL

def main():
    print("Starting metrics collection server...")
    
    # Run metrics server in a separate process
    server_process = multiprocessing.Process(target= run_metrics_server, args= (PROMETHEUS_PORT,))
    server_process.start()
    
    # Wait for server to start
    time.sleep(5)
    
    print("Collecting data for 1 minute...")
    # Collect data for 1 minute
    time.sleep(60)
    
    # Fetch and process data
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=1)
    
    metrics_data = fetch_metrics(PROMETHEUS_URL, start_time, end_time)
    df = process_data(metrics_data)
    
    print("\nCollected data:")
    print(df)
    
    analysis_results = analyze_data(df)
    print("\nData Analysis:")
    
    for key, value in analysis_results.items():
        print(f"{key}:{value:.2f}")
        
    # Stop the metrics server
    server_process.terminate()
    server_process.join()
    
if __name__ == "__main__":
    main()