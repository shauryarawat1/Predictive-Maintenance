import pytest
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.data_storage import Base, SystemMetrics, store_metrics, get_metrics
from src.config import DATABASE_URL

@pytest.fixture(scope="module")

def test_db():
    
    # Create a test database
    test_db_url = f"{DATABASE_URL}_test"
    engine = create_engine(test_db_url)
    Base.metadata.create_all(engine)
    
    # Create a new session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    # Clean up
    session.close()
    Base.metadata.drop_all(engine)
    
def test_store_metrics(test_db):
    # Create sample dataframe
    df = pd.DataFrame({
        "cpu_usage_percent": [50.0, 60.0],
        "memory_usage_percent": [70.0, 80.0],
        "disk_usage_percent": [30.0, 40.0]
    }, index = pd.date_range('2021-06-01', periods = 2, freq = 'min'))
    
    # Store the metrics
    store_metrics(df)
    
    # Verify that metrics are stored
    stored_metrics = test_db.query(SystemMetrics).all()
    assert len(stored_metrics) == 2
    
def test_get_metrics(test_db):
    
    # Add some test data
    start_time = datetime.utcnow()
    
    for i in range(5):
        metric = SystemMetrics(
            timestamp = start_time + timedelta(minutes=1),
            cpu_usage = 50.0 + i,
            memory_usage = 60.0 + i,
            disk_usage = 70.0 + i
        )
        
        test_db.add(metric)
        
    test_db.commit()
    
    # Retrieve metrics
    retrieved_metrics = get_metrics(start_time, start_time + timedelta(minutes = 10))
    assert len(retrieved_metrics) == 5
    
    # Verify retrieved data
    assert retrieved_metrics[0].cpu_usage == 50.0
    assert retrieved_metrics[-1].cpu_usage == 54.0