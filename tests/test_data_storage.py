import pytest
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.data_storage import Base, SystemMetrics, store_metrics, get_metrics
from src.config import DATABASE_URL

@pytest.fixture(scope="module")

def test_db():
    
    # Create a test database
    test_db_url = f"{DATABASE_URL}_test"
    
    engine = create_engine(test_db_url.rsplit('/', 1)[0] + '/postgres')
    
    conn = engine.connect()
    
    conn.execute(text("COMMIT"))
    conn.execute(text(f"CREATE DATABASE {test_db_url.rsplit('/', 1)[1]}"))
    conn.close()
    
    engine = create_engine(test_db_url)
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind = engine)
    session = Session()
    
    yield session
    
    session.close()
    
    conn = engine.connect()
    
    conn.execute(text("COMMIT"))
    conn.execute(text(f"DROP DATABASE {test_db_url.rsplit('/', 1)[1]}"))
    conn.close()
    
def test_store_metrics(test_db):
    # Create sample dataframe
    df = pd.DataFrame({
        "cpu_usage_percent": [50.0, 60.0],
        "memory_usage_percent": [70.0, 80.0],
        "disk_usage_percent": [30.0, 40.0],
        'network_io_sent_bytes': [1000, 2000],
        'network_io_recv_bytes': [1500, 2500],
        'disk_io_read_bytes': [500, 1000],
        'disk_io_write_bytes': [750, 1500],
        'network_io_sent_bytes_rate': [100, 200],
        'network_io_recv_bytes_rate': [150, 250],
        'disk_io_read_bytes_rate': [50, 100],
        'disk_io_write_bytes_rate': [75, 150]
    }, index = pd.date_range(start = datetime.now(timezone.utc), periods = 2, freq = "min"))
    
    # Store the metrics
    store_metrics(df)
    
    # Verify that metrics are stored
    stored_metrics = test_db.query(SystemMetrics).all()
    assert len(stored_metrics) == 2
    
def test_get_metrics(test_db):
    
    # Add some test data
    start_time = datetime.now(timezone.utc)
    
    for i in range(5):
        metric = SystemMetrics(
            timestamp = start_time + timedelta(minutes=1),
            cpu_usage = 50.0 + i,
            memory_usage = 60.0 + i,
            disk_usage = 70.0 + i,
            network_io_sent=1000 * i,
            network_io_recv=1500 * i,
            disk_io_read=500 * i,
            disk_io_write=750 * i,
            network_io_sent_rate=100 * i,
            network_io_recv_rate=150 * i,
            disk_io_read_rate=50 * i,
            disk_io_write_rate=75 * i
        )
        
        test_db.add(metric)
        
    test_db.commit()
    
    # Retrieve metrics
    retrieved_metrics = get_metrics(start_time, start_time + timedelta(minutes = 10))
    assert len(retrieved_metrics) == 5
    
    # Verify retrieved data
    assert retrieved_metrics[0].cpu_usage == 50.0
    assert retrieved_metrics[-1].cpu_usage == 54.0