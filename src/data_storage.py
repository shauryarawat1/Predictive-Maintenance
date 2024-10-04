from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.config import DATABASE_URL
import pandas as pd

Base = declarative_base()

# Defines the database table structure. Each attribute is a column in the table
class SystemMetrics(Base):
    __tablename__ = "system_metrics"
    
    id = Column(Integer, primary_key = True)
    timestamp = Column(DateTime, default= lambda: datetime.now(timezone.utc))
    cpu_usage = Column(Float)
    memory_usage = Column(Float)
    disk_usage = Column(Float)
    network_io_sent = Column(Float)
    network_io_recv = Column(Float)
    disk_io_read = Column(Float)
    disk_io_write = Column(Float)
    network_io_sent_rate = Column(Float)
    network_io_recv_rate = Column(Float)
    disk_io_read_rate = Column(Float)
    disk_io_write_rate = Column(Float)

    
# Set up database connection
engine = create_engine(DATABASE_URL)

# Creates table if does not exist before
Base.metadata.create_all(engine)

# Factory for database connections
Session = sessionmaker(bind=engine)

def store_metrics(df):
    # Store the processed metrics in the database
    
    # Create new database session
    session = Session()
    
    try:
        # Iterate over each row in dataframe
        for _, row in df.iterrows():
            
            # Create new object for each row
            metric = SystemMetrics(
                timestamp = row.name,
                cpu_usage = row["cpu_usage_percent"],
                memory_usage = row["memory_usage_percent"],
                disk_usage = row["disk_usage_percent"],
                network_io_sent=row.get('network_io_sent_bytes'),
                network_io_recv=row.get('network_io_recv_bytes'),
                disk_io_read=row.get('disk_io_read_bytes'),
                disk_io_write=row.get('disk_io_write_bytes'),
                network_io_sent_rate=row.get('network_io_sent_bytes_rate'),
                network_io_recv_rate=row.get('network_io_recv_bytes_rate'),
                disk_io_read_rate=row.get('disk_io_read_bytes_rate'),
                disk_io_write_rate=row.get('disk_io_write_bytes_rate')
            )
            
            session.add(metric)
            
        # Save the data
        session.commit()
        
    except Exception as e:
        session.rollback()
        raise e
    
    finally:
        session.close()
        
def get_metrics(start_time, end_time):
    # Receive metrics from database within an interval
    
    session = Session()
    
    try:
        
        # Run a query in database to get metrics within the time range
        metrics = session.query(SystemMetrics).filter(
            SystemMetrics.timestamp.between(start_time, end_time)
        ).all()
        
        return metrics
    
    finally:
        session.close()
        
