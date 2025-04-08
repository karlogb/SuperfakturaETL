"""
Configuration settings for the Superfaktura ETL pipeline.
"""
import os
from datetime import datetime
import tempfile

# API Authentication
SUPERFAKTURA_EMAIL = os.getenv("SUPERFAKTURA_EMAIL", "")
SUPERFAKTURA_APIKEY = os.getenv("SUPERFAKTURA_APIKEY", "")

# Data Paths - use a local directory
BASE_DATA_PATH = os.getenv("DATA_PATH", os.path.join(os.getcwd(), "/data/superfaktura/data"))

# Ensure base data directory exists
os.makedirs(BASE_DATA_PATH, exist_ok=True)

# BigQuery Settings
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")  # Zmeňte na váš projekt
DATASET_ID = os.getenv("BQ_DATASET_ID", "")  # Vyberte si názov datasetu

# BigQuery Table Names
BQ_TABLES = {
    # Main tables (processed through presentation layer)
    "client": "clients",
    "clientstat": "client_stats",
    "invoices": "invoices",
    "invoiceitem": "invoice_items",
    "expenses": "expenses",
    "expenses_categories": "expense_categories",
    
    # Tables for full extraction data
    "client_full": "clients_full",
    "clientstat_full": "client_stats_full",
    "invoices_full": "invoices_full",
    "invoiceitem_full": "invoice_items_full",
    "expenses_full": "expenses_full",
    
    # Tables for incremental extraction data
    "client_inc": "clients_inc",
    "clientstat_inc": "client_stats_inc",
    "invoices_inc": "invoices_inc",
    "invoiceitem_inc": "invoice_items_inc"
}

# Date Settings - Using a fixed date for the example
RUN_DATE = datetime.now()
RUN_DATE_KEY = RUN_DATE.strftime('%Y%m%d')

# Pipeline Modes
EXTRACT_FULL = True  # Set to False to only run incremental extraction
EXTRACT_INCREMENTAL = True  # Set to False to only run full extraction

# Logging Settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# BQ Credentials
BQ_CREDENTIALS_PATH = os.getenv("BQ_CREDENTIALS_PATH", os.path.join(BASE_DATA_PATH, "/keys/serviceAccount/serviceAccount.json"))