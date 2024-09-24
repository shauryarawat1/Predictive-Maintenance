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