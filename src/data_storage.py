from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
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

def store_metrics(df, session=None):
    close_session = False
    if session is None:
        session = Session()
        close_session = True
    try:
        for index, row in df.iterrows():
            metric = SystemMetrics(
                timestamp=index,
                cpu_usage=float(row['cpu_usage_percent']),
                memory_usage=float(row['memory_usage_percent']),
                disk_usage=float(row['disk_usage_percent']),
                network_io_sent=float(row['network_io_sent_bytes']),
                network_io_recv=float(row['network_io_recv_bytes']),
                disk_io_read=float(row['disk_io_read_bytes']),
                disk_io_write=float(row['disk_io_write_bytes']),
                network_io_sent_rate=float(row['network_io_sent_bytes_rate']),
                network_io_recv_rate=float(row['network_io_recv_bytes_rate']),
                disk_io_read_rate=float(row['disk_io_read_bytes_rate']),
                disk_io_write_rate=float(row['disk_io_write_bytes_rate'])
            )
            session.add(metric)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        if close_session:
            session.close()
        
def get_metrics(start_time, end_time, session=None):
    close_session = False
    if session is None:
        session = Session()
        close_session = True
    try:
        metrics = session.query(SystemMetrics).filter(
            SystemMetrics.timestamp.between(start_time, end_time)
        ).all()
        return metrics
    finally:
        if close_session:
            session.close()
        
