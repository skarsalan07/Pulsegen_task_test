# Configuration settings
import os
from datetime import datetime, timedelta

# App Configuration
APP_ID = 'in.swiggy.android'
LANG = 'en'
COUNTRY = 'in'

# Batch Processing Configuration
START_DATE = datetime(2025, 6, 1).date()
DAILY_BATCH_SIZE = 200

# Path Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
DB_DIR = os.path.join(DATA_DIR, 'database')
BATCH_STATUS_DIR = os.path.join(DATA_DIR, 'batch_status')
DB_PATH = os.path.join(DB_DIR, 'reviews.db')

# Create directories
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(BATCH_STATUS_DIR, exist_ok=True)