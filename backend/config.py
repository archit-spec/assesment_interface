"""Configuration module for the application.

This module handles all configuration settings including database connection,
API settings, and environment-specific configurations.
"""

import logging
import os
from typing import Optional
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Environment configuration
DEBUG_MODE = os.getenv("DEBUG", "False").lower() == "true"
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_WORKERS = 4

# Database configuration
def get_database_url() -> str:
    """Get database URL based on environment.
    
    Returns:
        Database connection URL string.
    """
    # Check if running on Railway
    is_railway = bool(os.getenv("RAILWAY_ENVIRONMENT"))
    
    if is_railway:
        # Use internal hostname for Railway PostgreSQL
        host = "postgres.railway.internal"
        port = os.getenv("PGPORT", "5432")
        db = os.getenv("PGDATABASE", "railway")
        user = os.getenv("PGUSER", "postgres")
        password = os.getenv("PGPASSWORD", "")
        
        return (
            f"postgresql://{user}:{password}@{host}:{port}/{db}"
        )
    
    # Use provided DATABASE_URL or default local
    return os.getenv(
        "DATABASE_URL",
        "postgresql://dumball:tammu123@db:5432/accha"
    )

# Set database URL
DATABASE_URL = get_database_url()

# File upload settings
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "uploads"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", "processed"))

# Create directories if they don't exist
UPLOAD_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)

logger.info("Configuration loaded successfully")
