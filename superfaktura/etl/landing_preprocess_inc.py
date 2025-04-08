import requests
import polars as pl
from datetime import datetime, timedelta
from pytz import timezone
import json
import os
import time
import logging
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from utils import ensure_directory
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set up API Credentials
email = config.SUPERFAKTURA_EMAIL
apikey = config.SUPERFAKTURA_APIKEY

def get_auth(api_name):
    """
    Generates an authorization dictionary for the specified API.
    Args:
        api_name (str): The name of the API for which to generate the authorization dictionary.
    Returns:
        dict: A dictionary containing the authorization header for the specified API.
              If the API name is not supported, the function returns an empty dictionary.
    """
    auth_dict = {}  # Initialize with empty dictionary
    
    if api_name == "superfaktura":
        auth_dict = {"Authorization": f"SFAPI email={email}&apikey={apikey}"}
    
    return auth_dict

def create_robust_session(retries=5, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)):
    """
    Creates a requests Session with automatic retries and backoff.
    
    Args:
        retries (int): Maximum number of retries
        backoff_factor (float): Backoff factor for exponential delay between retries
        status_forcelist (tuple): HTTP status codes that should trigger a retry
        
    Returns:
        requests.Session: A session with retry configuration
    """
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(['GET', 'POST']),
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

def _get_schema_from_json(json):
    """
    Generate a schema dictionary from a given JSON object.
    This function takes a JSON object and infers the schema of its contents.
    It converts any fields with a type of `pl.Null` or `pl.Boolean` to `pl.Utf8`.
    The resulting schema dictionary maps field names to their inferred types.
    Args:
        json (list): A list of dictionaries representing the JSON object.
    Returns:
        dict: A dictionary where keys are field names and values are their inferred types.
    """
    schema_dict = dict(pl.from_dicts(json, infer_schema_length=None).schema)
    schema = {}
    
    for name,type in schema_dict.items():
        if type == pl.Null:
            schema.update({name:pl.Utf8})
        elif type == pl.Boolean:
            schema.update({name:pl.Utf8})
        else:
            schema.update({name:type})
    return schema

def get_json_superfaktura_clients_increment(run_date, data_path):
    """
    Fetches and saves incrementally updated client data from the Superfaktura API.
    Uses robust session with retries and error handling for reliability.
    
    Args:
        run_date (datetime): The date for which the data is being fetched.
        data_path (str): The base path where the data will be saved.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request after all retries.
        IOError: If there is an issue writing the JSON file.
    """
    MAX_RETRIES = 5
    
    run_date_key = run_date.strftime('%Y%m%d')
    start_date = (run_date - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    end_date = (run_date.strftime('%Y-%m-%d %H:%M:%S'))

    url = "https://moja.superfaktura.sk/clients/index.json/"
    time_period = f"/modified:3/modified_since:{start_date}/modified_to{end_date}"
    
    # Create a robust session with automatic retries
    session = create_robust_session(retries=MAX_RETRIES, backoff_factor=0.5)
    
    try:
        # Get the total number of pages
        logger.info(f"Fetching total client increment page count for period {start_date} to {end_date}...")
        response = session.get(
            f'{url}listinfo:1{time_period}', 
            headers=get_auth("superfaktura"),
            timeout=30
        )
        response.raise_for_status()
        page_count = response.json()["pageCount"]
        logger.info(f"Total client increment pages to fetch: {page_count}")
        
        pages = []
        for p in range(page_count):
            page_num = p + 1
            try:
                # Add small delay to avoid overwhelming API
                time.sleep(0.2)
                
                logger.info(f"Fetching client increment page {page_num}/{page_count}")
                response = session.get(
                    f'{url}page:{page_num}{time_period}', 
                    headers=get_auth("superfaktura"),
                    timeout=30
                )
                response.raise_for_status()
                page_data = response.json()
                
                if not page_data:
                    logger.warning(f"Empty response for client increment page {page_num}")
                    continue
                    
                pages.extend(page_data)
                
                # Log progress
                if page_num % 10 == 0 or page_num == page_count:
                    logger.info(f"Completed {page_num}/{page_count} client increment pages")
                
            except Exception as e:
                logger.error(f"Error fetching client increment page {page_num}: {str(e)}")
                # Continue with next page instead of failing the entire process
                continue
        
        # Save results
        landing_dir = f"{data_path}/landing/client_increment/{run_date_key}"
        ensure_directory(landing_dir)
        json_file_path = f"{landing_dir}/client_increment_raw.json"
        
        with open(json_file_path, 'w') as json_file:
            json.dump(pages, json_file, indent=2)
        
        logger.info(f"Client increment data has been successfully written to {json_file_path} with page_count: {page_count}")
        print(f'Data has been successfully written to {json_file_path} with page_count: {page_count}')
        
    except Exception as e:
        logger.error(f"Failed to fetch client increment data: {str(e)}", exc_info=True)
        print(f"ERROR: Failed to fetch client increment data: {str(e)}")
        raise

def get_superfaktura_clients_increment(run_date, data_path):
    """
    Processes and saves Superfaktura client and client statistics increment data for a given run date.
    Args:
        run_date (datetime): The date for which the increment data is being processed.
        data_path (str): The base path where the data is stored and where the processed data will be saved.
    Raises:
        FileNotFoundError: If the JSON file containing the client increment data does not exist.
        json.JSONDecodeError: If there is an error decoding the JSON file.
        subprocess.CalledProcessError: If there is an error creating the directories for saving the processed data.
    Side Effects:
        Creates directories for saving the processed client and client statistics increment data.
        Writes the processed client and client statistics increment data to Parquet files.
        Prints the shapes of the written data.
    Example:
        >>> from datetime import datetime
        >>> run_date = datetime.strptime('2023-10-11', '%Y-%m-%d')
        >>> data_path = '/path/to/data'
        >>> get_superfaktura_clients_increment(run_date, data_path)
        Data has been successfully written to /path/to/data/preprocess/client_increment/20231011 with shape: (n, m)
        Data has been successfully written to /path/to/data/preprocess/clientstat_increment/20231011 with shape: (n, m)
    """
    run_date_key = run_date.strftime('%Y%m%d')
    json_file_path = f"{data_path}/landing/client_increment/{run_date_key}/client_increment_raw.json"
    save_path_client_increment = f"{data_path}/preprocess/client_increment/{run_date_key}"
    save_path_clientstat_increment = f"{data_path}/preprocess/clientstat_increment/{run_date_key}"
    
    with open(json_file_path, 'r') as json_file:
        pages = json.load(json_file)
    
    superfaktura_clients_client = [x["Client"] for x in pages]
    superfaktura_clients_clientstat = [x["ClientStat"] for x in pages]
    
    # Use ensure_directory instead of subprocess
    ensure_directory(save_path_client_increment)
    ensure_directory(save_path_clientstat_increment)
    
    superfaktura_clients_client_schema = _get_schema_from_json(superfaktura_clients_client)
    superfaktura_clients_clientstat_schema = _get_schema_from_json(superfaktura_clients_clientstat)
    
    # Save to BigQuery compatible format
    client_df = pl.from_dicts(superfaktura_clients_client, schema=superfaktura_clients_client_schema)
    client_df.write_parquet(f"{save_path_client_increment}/client_increment.parquet")
    
    clientstat_df = pl.from_dicts(superfaktura_clients_clientstat, schema=superfaktura_clients_clientstat_schema)
    clientstat_df.write_parquet(f"{save_path_clientstat_increment}/clientstat_increment.parquet")
    
    print(f'Data has been successfully written to {save_path_client_increment} with shape: {client_df.shape}')
    print(f'Data has been successfully written to {save_path_clientstat_increment} with shape: {clientstat_df.shape}')

def get_json_superfaktura_invoices_increment(run_date, data_path):
    """
    Fetches incremental invoice data from the Superfaktura API and saves it as a JSON file.
    Uses robust session with retries and error handling for reliability.
    
    Args:
        run_date (datetime): The date for which the incremental data is to be fetched.
        data_path (str): The base path where the JSON file will be saved.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request after all retries.
        IOError: If there is an issue writing the JSON file.
    """
    MAX_RETRIES = 5
    
    run_date_key = run_date.strftime('%Y%m%d')
    start_date = (run_date - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
    end_date = (run_date.strftime('%Y-%m-%d %H:%M:%S'))

    url = "https://moja.superfaktura.sk/invoices/index.json/"
    type_param = "type:regular|cancel|proforma|tax_document"
    time_period = f"/modified:3/modified_since:{start_date}/modified_to{end_date}"
    
    # Create a robust session with automatic retries
    session = create_robust_session(retries=MAX_RETRIES, backoff_factor=0.5)
    
    try:
        # Get the total number of pages
        logger.info(f"Fetching total invoice increment page count for period {start_date} to {end_date}...")
        response = session.get(
            f'{url}listinfo:1/{type_param}{time_period}', 
            headers=get_auth("superfaktura"),
            timeout=30
        )
        response.raise_for_status()
        page_count = response.json()["pageCount"]
        logger.info(f"Total invoice increment pages to fetch: {page_count}")
        
        pages = []
        for p in range(page_count):
            page_num = p + 1
            try:
                # Add small delay to avoid overwhelming API
                time.sleep(0.2)
                
                logger.info(f"Fetching invoice increment page {page_num}/{page_count}")
                response = session.get(
                    f'{url}{type_param}/page:{page_num}{time_period}', 
                    headers=get_auth("superfaktura"),
                    timeout=30
                )
                response.raise_for_status()
                page_data = response.json()
                
                if not page_data:
                    logger.warning(f"Empty response for invoice increment page {page_num}")
                    continue
                    
                pages.extend(page_data)
                
                # Log progress
                if page_num % 10 == 0 or page_num == page_count:
                    logger.info(f"Completed {page_num}/{page_count} invoice increment pages")
                
            except Exception as e:
                logger.error(f"Error fetching invoice increment page {page_num}: {str(e)}")
                # Continue with next page instead of failing the entire process
                continue
        
        # Save results
        landing_dir = f"{data_path}/landing/invoices_increment/{run_date_key}"
        ensure_directory(landing_dir)
        json_file_path = f"{landing_dir}/invoices_increment_raw.json"
        
        with open(json_file_path, 'w') as json_file:
            json.dump(pages, json_file, indent=2)
        
        logger.info(f"Invoice increment data has been successfully written to {json_file_path} with page_count: {page_count}")
        print(f'Data has been successfully written to {json_file_path} with page_count: {page_count}')
        
    except Exception as e:
        logger.error(f"Failed to fetch invoice increment data: {str(e)}", exc_info=True)
        print(f"ERROR: Failed to fetch invoice increment data: {str(e)}")
        raise

def get_superfaktura_invoices_increment(run_date, data_path):
    """
    Processes Superfaktura invoices incrementally for a given run date and saves the results in Parquet format.
    Args:
        run_date (datetime): The date for which the invoices are being processed.
        data_path (str): The base path where the data is stored and where the results will be saved.
    Raises:
        FileNotFoundError: If the JSON file containing the invoices is not found.
        json.JSONDecodeError: If there is an error decoding the JSON file.
        subprocess.CalledProcessError: If there is an error creating the directories for saving the results.
    Returns:
        None
    Example:
        >>> from datetime import datetime
        >>> run_date = datetime.strptime('2023-10-11', '%Y-%m-%d')
        >>> data_path = '/path/to/data'
        >>> get_superfaktura_invoices_increment(run_date, data_path)
    """
    run_date_key = run_date.strftime('%Y%m%d')
    json_file_path = f"{data_path}/landing/invoices_increment/{run_date_key}/invoices_increment_raw.json"
    save_path_invoices_increment = f"{data_path}/preprocess/invoices_increment/{run_date_key}"
    save_path_invoiceitem_increment = f"{data_path}/preprocess/invoiceitem_increment/{run_date_key}"

    with open(json_file_path, 'r') as json_file:
        pages = json.load(json_file)

    superfaktura_invoices_invoice = [x["Invoice"] for x in pages]
    superfaktura_invoices_invoiceitem = [x2 for x1 in pages for x2 in x1["InvoiceItem"]]

    # Use ensure_directory instead of subprocess
    ensure_directory(save_path_invoices_increment)
    ensure_directory(save_path_invoiceitem_increment)

    superfaktura_invoices_invoice_schema = _get_schema_from_json(superfaktura_invoices_invoice)
    superfaktura_invoices_invoiceitem_schema = _get_schema_from_json(superfaktura_invoices_invoiceitem)
    
    # Save to BigQuery compatible format
    invoices_df = pl.from_dicts(superfaktura_invoices_invoice, schema=superfaktura_invoices_invoice_schema)
    invoices_df.write_parquet(f"{save_path_invoices_increment}/invoices_increment.parquet")
    
    invoiceitem_df = pl.from_dicts(superfaktura_invoices_invoiceitem, schema=superfaktura_invoices_invoiceitem_schema)
    invoiceitem_df.write_parquet(f"{save_path_invoiceitem_increment}/invoiceitem_increment.parquet")
    
    print(f'Data has been successfully written to {save_path_invoices_increment} with shape: {invoices_df.shape}')
    print(f'Data has been successfully written to {save_path_invoiceitem_increment} with shape: {invoiceitem_df.shape}')

def get_json_superfaktura_expenses_categories(run_date, data_path):
    """
    Fetches expense categories from the Superfaktura API and saves them as a JSON file.
    Uses a robust session with retries for reliability.
    
    Args:
        run_date (datetime): The date when the function is run, used to format the run_date_key.
        data_path (str): The base path where the JSON file will be saved.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request after all retries.
        IOError: If there is an issue writing the JSON file.
    Example:
        get_json_superfaktura_expenses_categories(datetime.datetime.now(), '/path/to/data')
    """
    MAX_RETRIES = 5
    
    run_date_key = run_date.strftime('%Y%m%d')
    landing_dir = f"{data_path}/landing/expenses_categories"
    ensure_directory(landing_dir)
    json_file_path = f"{landing_dir}/expenses_categories_raw.json"
    
    # Create a robust session with automatic retries
    session = create_robust_session(retries=MAX_RETRIES, backoff_factor=0.5)
    
    try:
        logger.info("Fetching expense categories...")
        response = session.get(
            f'https://moja.superfaktura.sk/expenses/expense_categories', 
            headers=get_auth("superfaktura"),
            timeout=30
        )
        response.raise_for_status()
        pages = response.json()
        
        if not pages:
            logger.warning("Empty response for expense categories")
            raise ValueError("Empty response received from API")
            
        # Save all expense categories data to JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(pages, json_file, indent=2)
        
        logger.info(f"Expense categories data has been successfully written to {json_file_path}")
        print(f'Data has been successfully written to {json_file_path}')
        
    except Exception as e:
        logger.error(f"Failed to fetch expense categories data: {str(e)}", exc_info=True)
        print(f"ERROR: Failed to fetch expense categories data: {str(e)}")
        raise

def get_superfaktura_expenses_categories(run_date, data_path):
    """
    Processes and saves Superfaktura expenses categories data.
    Args:
        run_date (datetime): The date when the function is run, used to format the run_date_key.
        data_path (str): The base path where the data files are located and where the processed data will be saved.
    Returns:
        None
    This function reads a JSON file containing expenses categories data, processes it to flatten the structure,
    and saves the processed data as a Parquet file. The JSON file is expected to be located at
    '{data_path}/landing/expenses_categories/expenses_categories_raw.json'. The processed data is saved to
    '{data_path}/preprocess/expenses_categories/expenses_categories.parquet'.
    """
    with open(f"{data_path}/landing/expenses_categories/expenses_categories_raw.json", 'r') as json_file:
        pages = json.load(json_file)
    
    expense_categories_flat = []
    
    def _category_dict(parent, child):
        return {
            "id": child["id"],
            "parent_id": parent["id"] if parent else "",
            "parent_name": parent["name"] if parent else "",
            "name": child["name"],
            "level": 1 if not parent else 2,
            "fullname": child["name"] if not parent else f"{parent['name']} / {child['name']}"
        }
    
    for category in pages["ExpenseCategory"]:
        expense_categories_flat.append(_category_dict(None, category))
        if "children" in category and category["children"]:
            for child in category["children"]:
                expense_categories_flat.append(_category_dict(category, child))
    
    save_path = f"{data_path}/preprocess/expenses_categories"
    ensure_directory(save_path)
    
    expense_categories_df = pl.from_dicts(expense_categories_flat)
    expense_categories_df.write_parquet(f"{save_path}/expenses_categories.parquet")
    
    print(f'Data has been successfully written to {save_path} with shape: {expense_categories_df.shape}')

def main():
    """
    Main function to preprocess incremental Superfaktura data.
    """
    # Import config from main module to get current settings
    import config
    data_path = config.BASE_DATA_PATH  # Use path from config
    run_date = datetime.now(timezone('Europe/Bratislava'))
    
    get_json_superfaktura_clients_increment(run_date, data_path)
    get_superfaktura_clients_increment(run_date, data_path)
    
    get_json_superfaktura_invoices_increment(run_date, data_path)
    get_superfaktura_invoices_increment(run_date, data_path)
    
    get_json_superfaktura_expenses_categories(run_date, data_path)
    get_superfaktura_expenses_categories(run_date, data_path)

if __name__ == "__main__":
    main()