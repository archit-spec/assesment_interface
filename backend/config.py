import os
from pathlib import Path
import logging

# Database configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Check if we're running on Railway
is_railway = os.getenv("RAILWAY_ENVIRONMENT") == "production"

# Log environment
logger.info(f"Running on Railway: {is_railway}")
logger.info("Environment variables:")
for var in ["DATABASE_URL", "DATABASE_PUBLIC_URL", "PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGPORT"]:
    logger.info(f"{var}: {os.getenv(var, 'not set')}")

if is_railway:
    # Use Railway's internal database URL for better performance and security
    DATABASE_URL = os.getenv("DATABASE_URL")  # This uses the internal connection URL
    if not DATABASE_URL:
        # Fallback to constructing URL from individual vars
        db_user = os.getenv("PGUSER", "postgres")
        db_pass = os.getenv("PGPASSWORD")
        db_host = os.getenv("PGHOST", "postgres.railway.internal")
        db_port = os.getenv("PGPORT", "5432")
        db_name = os.getenv("PGDATABASE", "railway")
        DATABASE_URL = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    logger.info("Using Railway database configuration")
else:
    # Use Docker Compose configuration
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dumball:tammu123@db:5432/accha")
    logger.info("Using Docker Compose database configuration")

logger.info(f"Final DATABASE_URL: {DATABASE_URL}")

# File storage configuration
UPLOAD_DIR = Path("uploads")
PROCESSED_DIR = Path("processed")

# Ensure directories exist
UPLOAD_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)

# Processing configuration
BATCH_SIZE = 1000
MAX_WORKERS = 4

# API configuration
API_HOST = "0.0.0.0"
API_PORT = 8000
DEBUG_MODE = True
