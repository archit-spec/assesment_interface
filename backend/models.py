"""Database models and schemas for the application.

This module contains SQLAlchemy models for database tables and Pydantic models
for API request/response validation.
"""

import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy import JSON, Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config import DATABASE_URL

Base = declarative_base()


class UnprocessedData(Base):
    """Model for storing raw unprocessed data files.

    Attributes:
        id: Unique identifier for the record
        filename: Name of the uploaded file
        file_type: Type of the file (mtr or payment)
        upload_timestamp: When the file was uploaded
        raw_data: JSON representation of the raw data
        status: Current processing status
        error_message: Error message if processing failed
    """

    __tablename__ = "unprocessed"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, index=True)
    file_type = Column(String)  # 'mtr' or 'payment'
    upload_timestamp = Column(DateTime, default=datetime.utcnow)
    raw_data = Column(JSON)
    status = Column(String, default="pending")  # pending, processing, failed, processed
    error_message = Column(Text, nullable=True)


class ProcessedData(Base):
    """Model for storing processed data files.

    Attributes:
        id: Unique identifier for the record
        unprocessed_id: Reference to the original unprocessed record
        processing_timestamp: When the data was processed
        processed_data: JSON representation of the processed data
        summary: Summary statistics of the processed data
        status: Processing status
        file_type: Type of the processed file
    """

    __tablename__ = "processed"

    id = Column(Integer, primary_key=True, index=True)
    unprocessed_id = Column(Integer, index=True)
    processing_timestamp = Column(DateTime, default=datetime.utcnow)
    processed_data = Column(JSON)
    summary = Column(JSON)
    status = Column(String)
    file_type = Column(String)  # 'mtr' or 'payment'


class UnprocessedDataResponse(BaseModel):
    """API response model for unprocessed data records."""

    id: int
    filename: str
    file_type: str
    upload_timestamp: datetime
    status: str
    error_message: Optional[str] = None

    class Config:
        """Pydantic configuration for ORM mode."""

        from_attributes = True


class ProcessedDataResponse(BaseModel):
    """API response model for processed data records."""

    id: int
    unprocessed_id: int
    processing_timestamp: datetime
    summary: dict
    status: str
    file_type: str

    class Config:
        """Pydantic configuration for ORM mode."""

        from_attributes = True


class PaginatedResponse(BaseModel):
    """Generic paginated response model."""

    items: List[Any]
    total: int
    page: int
    size: int
    pages: int


class TransactionTypeStats(BaseModel):
    """Statistics for a specific transaction type."""

    count: int
    total_amount: float


class TransactionSummary(BaseModel):
    """Summary of transaction statistics."""

    total_records: int
    total_amount: float
    transaction_types: Dict[str, TransactionTypeStats]
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class TransactionQuery(BaseModel):
    """Query parameters for transaction filtering."""

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
        "application_name": "backend",
    }
    # Only add SSL requirements for Railway
    if is_railway:
        connect_args.update(
            {
                "sslmode": "prefer",  # Allow both SSL and non-SSL for internal connections
                "options": "-c search_path=public",
            }
        )
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=300,
        connect_args=connect_args,
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
    """Provide a transactional scope around a series of operations.

    Yields:
        Session: Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
