"""
Module for uploading Superfaktura data to BigQuery.
This module contains functions to upload both full and incremental data extracts to BigQuery.
"""
import os
import logging
import datetime
import glob
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import polars as pl
import config
from utils import ensure_directory

# Set up logging
logger = logging.getLogger(__name__)

# Set up BQ Credentials
credentials_path = config.BQ_CREDENTIALS_PATH

class BigQueryUploader:
    """
    Handles uploading of Superfaktura data to BigQuery.
    This class provides methods to upload both full and incremental data extracts to BigQuery.
    """
    
    def __init__(self, project_id=None, dataset_id=None):
        """
        Initialize the BigQuery uploader.
        
        Args:
            project_id (str, optional): Google Cloud project ID
            dataset_id (str, optional): BigQuery dataset ID
        """
        self.project_id = project_id or config.PROJECT_ID
        self.dataset_id = dataset_id or config.DATASET_ID
        
        # Explicitne použite súbor key.json z dátového priečinka
        credentials_path = os.path.join(config.BASE_DATA_PATH, "key.json")
        
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(
            credentials=credentials,
            project=self.project_id,
        )
        
        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self):
        """
        Ensures that the specified dataset exists, creates it if it doesn't.
        """
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            # Dataset does not exist, so create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"  # Set the dataset location
            dataset = self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def upload_dataframe(self, df, table_name, write_disposition="WRITE_TRUNCATE"):
        """
        Uploads a Polars DataFrame to BigQuery.
        
        Args:
            df (pl.DataFrame): Polars DataFrame to upload
            table_name (str): Name of the destination table
            write_disposition (str): Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
            
        Returns:
            google.cloud.bigquery.job.LoadJob: The load job
        """
        # Convert Polars DataFrame to Pandas for BigQuery compatibility
        logger.info(f"Converting Polars DataFrame to Pandas for {table_name}")
        pandas_df = df.to_pandas()
        
        # Define the load job configuration
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ],
        )
        
        # Start the load job
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        logger.info(f"Uploading DataFrame to BigQuery table {self.dataset_id}.{table_name}")
        job = self.client.load_table_from_dataframe(
            pandas_df, 
            table_ref, 
            job_config=job_config
        )
        
        # Wait for the job to complete
        job.result()
        
        # Log the outcome
        table = self.client.get_table(table_ref)
        logger.info(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {self.dataset_id}.{table_name}"
        )
        
        return job
    
    def upload_parquet_file(self, parquet_file, table_name, write_disposition="WRITE_TRUNCATE"):
        """
        Uploads a Parquet file to BigQuery.
        
        Args:
            parquet_file (str): Path to the Parquet file
            table_name (str): Name of the destination table
            write_disposition (str): Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
            
        Returns:
            google.cloud.bigquery.job.LoadJob: The load job
        """
        logger.info(f"Reading parquet file: {parquet_file}")
        # Read the parquet file using Polars
        df = pl.read_parquet(parquet_file)
        
        # Upload the DataFrame
        return self.upload_dataframe(df, table_name, write_disposition)
    
    def upload_from_present(self, data_path):
        """
        Uploads processed data from the presentation layer to BigQuery.
        
        Args:
            data_path (str): Base data path
        """
        logger.info("Uploading data from presentation layer to BigQuery")
        
        # Client data
        client_file = f"{data_path}/present/client/client.parquet"
        if os.path.exists(client_file):
            logger.info(f"Uploading client data from {client_file}")
            self.upload_parquet_file(client_file, config.BQ_TABLES["client"])
        else:
            logger.warning(f"Client file does not exist: {client_file}")
        
        # Client statistics data
        clientstat_file = f"{data_path}/present/clientstat/clientstat.parquet"
        if os.path.exists(clientstat_file):
            logger.info(f"Uploading client statistics data from {clientstat_file}")
            self.upload_parquet_file(clientstat_file, config.BQ_TABLES["clientstat"])
        else:
            logger.warning(f"Client statistics file does not exist: {clientstat_file}")
        
        # Invoices data
        invoices_file = f"{data_path}/present/invoices/invoices.parquet"
        if os.path.exists(invoices_file):
            logger.info(f"Uploading invoices data from {invoices_file}")
            self.upload_parquet_file(invoices_file, config.BQ_TABLES["invoices"])
        else:
            logger.warning(f"Invoices file does not exist: {invoices_file}")
        
        # Invoice item data
        invoiceitem_file = f"{data_path}/present/invoiceitem/invoiceitem.parquet"
        if os.path.exists(invoiceitem_file):
            logger.info(f"Uploading invoice item data from {invoiceitem_file}")
            self.upload_parquet_file(invoiceitem_file, config.BQ_TABLES["invoiceitem"])
        else:
            logger.warning(f"Invoice item file does not exist: {invoiceitem_file}")
        
        # Expenses data
        expenses_file = f"{data_path}/present/expenses/expenses.parquet"
        if os.path.exists(expenses_file):
            logger.info(f"Uploading expenses data from {expenses_file}")
            self.upload_parquet_file(expenses_file, config.BQ_TABLES["expenses"])
        else:
            logger.warning(f"Expenses file does not exist: {expenses_file}")
        
        # Expense categories data
        categories_file = f"{data_path}/preprocess/expenses_categories/expenses_categories.parquet"
        if os.path.exists(categories_file):
            logger.info(f"Uploading expense categories data from {categories_file}")
            self.upload_parquet_file(categories_file, config.BQ_TABLES["expenses_categories"])
        else:
            logger.warning(f"Expense categories file does not exist: {categories_file}")
    
    def upload_full_extract(self, data_path, run_date=None):
        """
        Uploads the full extract data directly to BigQuery (without going through presentation layer).
        
        Args:
            data_path (str): Base data path
            run_date (datetime, optional): Run date to use for directory naming
        """
        run_date = run_date or datetime.datetime.now()
        run_date_key = run_date.strftime('%Y%m%d')
        
        logger.info(f"Uploading full extract data for {run_date_key} to BigQuery")
        
        # Client data
        client_file = f"{data_path}/preprocess/client_all/{run_date_key}/client_all.parquet"
        if os.path.exists(client_file):
            logger.info(f"Uploading client data from {client_file}")
            self.upload_parquet_file(client_file, f"{config.BQ_TABLES['client']}_full")
        else:
            logger.warning(f"Client file does not exist: {client_file}")
        
        # Client statistics data
        clientstat_file = f"{data_path}/preprocess/clientstat_all/{run_date_key}/clientstat_all.parquet"
        if os.path.exists(clientstat_file):
            logger.info(f"Uploading client statistics data from {clientstat_file}")
            self.upload_parquet_file(clientstat_file, f"{config.BQ_TABLES['clientstat']}_full")
        else:
            logger.warning(f"Client statistics file does not exist: {clientstat_file}")
        
        # Invoices data
        invoices_file = f"{data_path}/preprocess/invoices_all/{run_date_key}/invoices_all.parquet"
        if os.path.exists(invoices_file):
            logger.info(f"Uploading invoices data from {invoices_file}")
            self.upload_parquet_file(invoices_file, f"{config.BQ_TABLES['invoices']}_full")
        else:
            logger.warning(f"Invoices file does not exist: {invoices_file}")
        
        # Invoice item data
        invoiceitem_file = f"{data_path}/preprocess/invoiceitem_all/{run_date_key}/invoiceitem_all.parquet"
        if os.path.exists(invoiceitem_file):
            logger.info(f"Uploading invoice item data from {invoiceitem_file}")
            self.upload_parquet_file(invoiceitem_file, f"{config.BQ_TABLES['invoiceitem']}_full")
        else:
            logger.warning(f"Invoice item file does not exist: {invoiceitem_file}")
        
        # Expenses data
        expenses_file = f"{data_path}/preprocess/expenses_all/expenses_all.parquet"
        if os.path.exists(expenses_file):
            logger.info(f"Uploading expenses data from {expenses_file}")
            self.upload_parquet_file(expenses_file, f"{config.BQ_TABLES['expenses']}_full")
        else:
            logger.warning(f"Expenses file does not exist: {expenses_file}")
    
    def upload_incremental_extract(self, data_path, run_date=None):
        """
        Uploads the incremental extract data directly to BigQuery (without going through presentation layer).
        
        Args:
            data_path (str): Base data path
            run_date (datetime, optional): Run date to use for directory naming
        """
        run_date = run_date or datetime.datetime.now()
        run_date_key = run_date.strftime('%Y%m%d')
        
        logger.info(f"Uploading incremental extract data for {run_date_key} to BigQuery")
        
        # Client data
        client_file = f"{data_path}/preprocess/client_increment/{run_date_key}/client_increment.parquet"
        if os.path.exists(client_file):
            logger.info(f"Uploading client data from {client_file}")
            self.upload_parquet_file(client_file, f"{config.BQ_TABLES['client']}_inc")
        else:
            logger.warning(f"Client file does not exist: {client_file}")
        
        # Client statistics data
        clientstat_file = f"{data_path}/preprocess/clientstat_increment/{run_date_key}/clientstat_increment.parquet"
        if os.path.exists(clientstat_file):
            logger.info(f"Uploading client statistics data from {clientstat_file}")
            self.upload_parquet_file(clientstat_file, f"{config.BQ_TABLES['clientstat']}_inc")
        else:
            logger.warning(f"Client statistics file does not exist: {clientstat_file}")
        
        # Invoices data
        invoices_file = f"{data_path}/preprocess/invoices_increment/{run_date_key}/invoices_increment.parquet"
        if os.path.exists(invoices_file):
            logger.info(f"Uploading invoices data from {invoices_file}")
            self.upload_parquet_file(invoices_file, f"{config.BQ_TABLES['invoices']}_inc")
        else:
            logger.warning(f"Invoices file does not exist: {invoices_file}")
        
        # Invoice item data
        invoiceitem_file = f"{data_path}/preprocess/invoiceitem_increment/{run_date_key}/invoiceitem_increment.parquet"
        if os.path.exists(invoiceitem_file):
            logger.info(f"Uploading invoice item data from {invoiceitem_file}")
            self.upload_parquet_file(invoiceitem_file, f"{config.BQ_TABLES['invoiceitem']}_inc")
        else:
            logger.warning(f"Invoice item file does not exist: {invoiceitem_file}")

def upload_all_data_to_bigquery(data_path=None, project_id=None, dataset_id=None, run_date=None):
    """
    Uploads all Superfaktura data to BigQuery (presentation, full and incremental).
    
    Args:
        data_path (str, optional): Base data path
        project_id (str, optional): Google Cloud project ID
        dataset_id (str, optional): BigQuery dataset ID
        run_date (datetime, optional): Run date to use for directory naming
    """
    data_path = data_path or config.BASE_DATA_PATH
    run_date = run_date or datetime.datetime.now()
    
    try:
        # Initialize the BigQuery uploader
        uploader = BigQueryUploader(project_id, dataset_id)
        
        # Upload from the presentation layer (combined data)
        uploader.upload_from_present(data_path)
        
        # Upload the full extract data (raw)
        uploader.upload_full_extract(data_path, run_date)
        
        # Upload the incremental extract data (raw)
        uploader.upload_incremental_extract(data_path, run_date)
        
        logger.info("All data uploaded to BigQuery successfully")
    except Exception as e:
        logger.error(f"Error uploading data to BigQuery: {str(e)}")
        raise