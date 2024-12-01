# model for the datbase
from datetime import datetime, date
from typing import List, Optional, Any, Dict
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from config import DATABASE_URL
import logging
import os

Base = declarative_base()


class UnprocessedData(Base):
    __tablename__ = "unprocessed"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, index=True)
    file_type = Column(String)  # 'mtr' or 'payment'
    upload_timestamp = Column(DateTime, default=datetime.utcnow)
    raw_data = Column(JSON)
    status = Column(String, default="pending")  # pending, processing, failed, processed
    error_message = Column(Text, nullable=True)


class ProcessedData(Base):
    __tablename__ = "processed"

    id = Column(Integer, primary_key=True, index=True)
    unprocessed_id = Column(Integer, index=True)
    processing_timestamp = Column(DateTime, default=datetime.utcnow)
    processed_data = Column(JSON)
    summary = Column(JSON)
    status = Column(String)
    file_type = Column(String)  # 'mtr' or 'payment'


# Pydantic models for API responses
class UnprocessedDataResponse(BaseModel):
    id: int
    filename: str
    file_type: str
    upload_timestamp: datetime
    status: str
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class ProcessedDataResponse(BaseModel):
    id: int
    unprocessed_id: int
    processing_timestamp: datetime
    summary: dict
    status: str
    file_type: str

    class Config:
        from_attributes = True


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    size: int
    pages: int


class TransactionTypeStats(BaseModel):
    count: int
    total_amount: float


class TransactionSummary(BaseModel):
    total_records: int
    total_amount: float
    transaction_types: Dict[str, TransactionTypeStats]
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class TransactionQuery(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    transaction_type: Optional[str] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    page: int = 1
    size: int = 10


# Database setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Attempting to connect to database with URL: {DATABASE_URL}")

try:
    # Check if we're running on Railway
    is_railway = os.getenv("RAILWAY_ENVIRONMENT") == "production"
    
    connect_args = {
        "connect_timeout": 30,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "application_name": "backend"
    }
    
    # Only add SSL requirements for Railway
    if is_railway:
        connect_args.update({
            "sslmode": "prefer",
            "options": "-c search_path=public"
        })
    
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=300,
        connect_args=connect_args
    )
    logger.info("Database engine created successfully")
except Exception as e:
    logger.error(f"Error creating engine: {e}")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create all tables
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")
except Exception as e:
    logger.error(f"Error creating tables: {e}")
    raise


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
