import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.data_storage import Base, SystemMetrics, store_metrics, get_metrics
from src.config import DATABASE_URL
import uuid

@pytest.fixture(scope="module")
def test_db():
    test_db_name = f"test_db_{uuid.uuid4().hex}"
    test_db_url = f"{DATABASE_URL.rsplit('/', 1)[0]}/postgres"
    engine = create_engine(test_db_url)
    with engine.connect() as conn:
        conn.execute(text("COMMIT"))
        conn.execute(text(f"CREATE DATABASE {test_db_name}"))
    
    test_db_url = f"{DATABASE_URL.rsplit('/', 1)[0]}/{test_db_name}"
    engine = create_engine(test_db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    session.close()
    engine.dispose()
    
    with create_engine(f"{DATABASE_URL.rsplit('/', 1)[0]}/postgres").connect() as conn:
        conn.execute(text("COMMIT"))
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_name}"))

def test_store_metrics(test_db):
    df = pd.DataFrame({
        'cpu_usage_percent': [50.0, 60.0],
        'memory_usage_percent': [70.0, 80.0],
        'disk_usage_percent': [30.0, 40.0],
        'network_io_sent_bytes': [1000, 2000],
        'network_io_recv_bytes': [1500, 2500],
        'disk_io_read_bytes': [500, 1000],
        'disk_io_write_bytes': [750, 1500],
        'network_io_sent_bytes_rate': [100, 200],
        'network_io_recv_bytes_rate': [150, 250],
        'disk_io_read_bytes_rate': [50, 100],
        'disk_io_write_bytes_rate': [75, 150]
    }, index=pd.date_range(start=datetime.now(timezone.utc), periods=2, freq='min'))

    store_metrics(df, test_db)

    stored_metrics = test_db.query(SystemMetrics).all()
    assert len(stored_metrics) == 2

def test_get_metrics(test_db):
    
    test_db.query(SystemMetrics).delete()
    test_db.commit()
    
    start_time = datetime.now(timezone.utc)
    for i in range(5):
        metric = SystemMetrics(
            timestamp=start_time + timedelta(minutes=i),
            cpu_usage=50.0 + i,
            memory_usage=60.0 + i,
            disk_usage=70.0 + i,
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

    retrieved_metrics = get_metrics(start_time, start_time + timedelta(minutes=10), test_db)
    assert len(retrieved_metrics) == 5