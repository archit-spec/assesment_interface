import os
from pathlib import Path

# Database configuration
DATABASE_URL = "postgresql://dumball:tammu123@db:5432/accha"

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
