import datetime
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.config import DATABASE_URL

Base = declarative_base()

# Defines the database table structure. Each attribute is a column in the table
class SystemMetrics(Base):
    __tablename__ = "system_metrics"
    
    id = Column(Integer, primary_key = True)
    timestamp = Column(DateTime, default= datetime.utcnow)
    cpu_usage = Column(Float)
    memory_usage = Column(Float)
    disk_usage = Column(Float)