"""
Main script for running the Superfaktura ETL pipeline.
"""
import os
import logging
import argparse
from datetime import datetime
import sys

# Importing modules from the current project
import config
from utils import ensure_directory, get_timestamp
from etl.bigquery_uploader import upload_all_data_to_bigquery
from etl.landing_preprocess_all import (
    get_json_superfaktura_clients_all,
    get_superfaktura_clients_all,
    get_json_superfaktura_invoices_all,
    get_superfaktura_invoices_all,
    get_json_superfaktura_expenses_all,
    get_superfaktura_expenses_all,
    get_json_superfaktura_expenses_categories,
    get_superfaktura_expenses_categories
)
from etl.landing_preprocess_inc import (
    get_json_superfaktura_clients_increment,
    get_superfaktura_clients_increment,
    get_json_superfaktura_invoices_increment,
    get_superfaktura_invoices_increment
)
from etl.present import (
    present_client,
    present_clientstat,
    present_invoices,
    present_invoiceitem,
    present_expenses
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{config.BASE_DATA_PATH}/superfaktura_etl_{config.RUN_DATE_KEY}.log")
    ]
)
logger = logging.getLogger(__name__)

def run_full_extraction(run_date, data_path):
    """
    Runs the full data extraction and processing pipeline.
    
    Args:
        run_date (datetime): The date for the extraction run
        data_path (str): The base path for data storage
    """
    logger.info("Starting full data extraction...")
    
    try:
        # Extract and process clients
        get_json_superfaktura_clients_all(run_date, data_path)
        get_superfaktura_clients_all(run_date, data_path)
        
        # Extract and process invoices
        get_json_superfaktura_invoices_all(run_date, data_path)
        get_superfaktura_invoices_all(run_date, data_path)
        
        # Extract and process expenses categories
        get_json_superfaktura_expenses_categories(run_date, data_path)
        get_superfaktura_expenses_categories(run_date, data_path)
        
        # Extract and process expenses
        try:
            get_json_superfaktura_expenses_all(run_date, data_path)
            get_superfaktura_expenses_all(run_date, data_path)
        except Exception as e:
            logger.warning(f"Error in expenses extraction: {str(e)}")
        
        logger.info("Full data extraction completed successfully")
    except Exception as e:
        logger.error(f"Error in full data extraction: {str(e)}")
        raise

def run_incremental_extraction(run_date, data_path):
    """
    Runs the incremental data extraction and processing pipeline.
    
    Args:
        run_date (datetime): The date for the extraction run
        data_path (str): The base path for data storage
    """
    logger.info("Starting incremental data extraction...")
    
    try:
        # Extract and process clients increment
        get_json_superfaktura_clients_increment(run_date, data_path)
        get_superfaktura_clients_increment(run_date, data_path)
        
        # Extract and process invoices increment
        get_json_superfaktura_invoices_increment(run_date, data_path)
        get_superfaktura_invoices_increment(run_date, data_path)
        
        logger.info("Incremental data extraction completed successfully")
    except Exception as e:
        logger.error(f"Error in incremental data extraction: {str(e)}")
        raise

def run_presentation_layer(data_path):
    """
    Runs the presentation layer processing.
    
    Args:
        data_path (str): The base path for data storage
    """
    logger.info("Starting presentation layer processing...")
    
    try:
        # Process and present client data
        present_client(data_path)
        present_clientstat(data_path)
        
        # Process and present invoice data
        present_invoices(data_path)
        present_invoiceitem(data_path)
        
        # Process and present expenses data
        try:
            present_expenses(data_path)
        except Exception as e:
            logger.warning(f"Error in presenting expenses: {str(e)}")
        
        logger.info("Presentation layer processing completed successfully")
    except Exception as e:
        logger.error(f"Error in presentation layer processing: {str(e)}")
        raise

def run_bigquery_upload(data_path, project_id=None, dataset_id=None):
    """
    Uploads all processed data to BigQuery.
    
    Args:
        data_path (str): The base path for data storage
        project_id (str, optional): Google Cloud project ID
        dataset_id (str, optional): BigQuery dataset ID
    """
    logger.info("Starting BigQuery upload...")
    
    try:
        upload_all_data_to_bigquery(data_path, project_id, dataset_id, config.RUN_DATE)
        logger.info("BigQuery upload completed successfully")
    except Exception as e:
        logger.error(f"Error in BigQuery upload: {str(e)}")
        raise

def run_full_pipeline(run_date=None, data_path=None, project_id=None, dataset_id=None):
    """
    Runs the full ETL pipeline including extraction, processing, and BigQuery upload.
    
    Args:
        run_date (datetime, optional): The date for the extraction run
        data_path (str, optional): The base path for data storage
        project_id (str, optional): Google Cloud project ID
        dataset_id (str, optional): BigQuery dataset ID
    """
    # Use default values from config if not provided
    run_date = run_date or config.RUN_DATE
    data_path = data_path or config.BASE_DATA_PATH
    
    logger.info(f"Starting Superfaktura ETL pipeline at {get_timestamp()}")
    logger.info(f"Run date: {run_date.strftime('%Y-%m-%d')}")
    logger.info(f"Data path: {data_path}")
    
    try:
        # Run full extraction if configured
        if config.EXTRACT_FULL:
            run_full_extraction(run_date, data_path)
        
        # Run incremental extraction if configured
        if config.EXTRACT_INCREMENTAL:
            run_incremental_extraction(run_date, data_path)
        
        # Run presentation layer processing
        run_presentation_layer(data_path)
        
        # Upload to BigQuery
        run_bigquery_upload(data_path, project_id, dataset_id)
        
        logger.info(f"Superfaktura ETL pipeline completed successfully at {get_timestamp()}")
    except Exception as e:
        logger.error(f"Error in Superfaktura ETL pipeline: {str(e)}")
        logger.error(f"Pipeline failed at {get_timestamp()}")
        raise

def parse_arguments():
    """
    Parses command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Superfaktura ETL Pipeline")
    
    parser.add_argument(
        "--mode",
        choices=["full", "incremental", "both"],
        default="both",
        help="Extraction mode: full, incremental, or both"
    )
    
    parser.add_argument(
        "--data-path",
        default=config.BASE_DATA_PATH,
        help="Base path for data storage"
    )
    
    parser.add_argument(
        "--project-id",
        default=config.PROJECT_ID,
        help="Google Cloud project ID"
    )
    
    parser.add_argument(
        "--dataset-id",
        default=config.DATASET_ID,
        help="BigQuery dataset ID"
    )
    
    parser.add_argument(
        "--no-bigquery",
        action="store_true",
        help="Skip BigQuery upload"
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_arguments()
    
    # Update configuration based on arguments
    config.BASE_DATA_PATH = args.data_path
    config.PROJECT_ID = args.project_id
    config.DATASET_ID = args.dataset_id
    
    if args.mode == "full":
        config.EXTRACT_FULL = True
        config.EXTRACT_INCREMENTAL = False
    elif args.mode == "incremental":
        config.EXTRACT_FULL = False
        config.EXTRACT_INCREMENTAL = True
    else:  # both
        config.EXTRACT_FULL = True
        config.EXTRACT_INCREMENTAL = True
    
    # Run the pipeline
    try:
        if args.no_bigquery:
            # Run extraction and processing without BigQuery upload
            run_date = config.RUN_DATE
            data_path = config.BASE_DATA_PATH
            
            if config.EXTRACT_FULL:
                run_full_extraction(run_date, data_path)
            
            if config.EXTRACT_INCREMENTAL:
                run_incremental_extraction(run_date, data_path)
            
            run_presentation_layer(data_path)
        else:
            # Run the full pipeline including BigQuery upload
            run_full_pipeline()
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)