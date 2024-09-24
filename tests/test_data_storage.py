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