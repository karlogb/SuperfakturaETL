import requests, subprocess
import polars as pl
from datetime import datetime
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
                        Currently supports "superfaktura".
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
    Generate a schema dictionary from a JSON object.
    This function takes a JSON object and infers the schema of its contents.
    It converts the schema to a dictionary where the keys are the field names
    and the values are the inferred data types. If the inferred type is `Null`
    or `Boolean`, it converts them to `Utf8`.
    Args:
        json (list): A list of dictionaries representing the JSON object.
    Returns:
        dict: A dictionary representing the schema with field names as keys
              and inferred data types as values.
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

def get_json_superfaktura_clients_all(run_date, data_path):
    """
    Fetches all client data from the SuperFaktura API and saves it as a JSON file.
    Uses robust session with retries and batch processing for reliability.

    Args:
        run_date (datetime): The date of the run, used to create a unique directory for the output file.
        data_path (str): The base path where the JSON file will be saved.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request after all retries.
        IOError: If there is an issue writing the JSON file.
    Example:
        get_json_superfaktura_clients_all(datetime.now(), '/path/to/data')
    """
    MAX_RETRIES = 5

    run_date_key = run_date.strftime('%Y%m%d')
    landing_dir = f"{data_path}/landing/client_all/{run_date_key}"
    ensure_directory(landing_dir)
    json_file_path = f"{landing_dir}/client_all_raw.json"

    # Create a robust session with automatic retries
    session = create_robust_session(retries=MAX_RETRIES, backoff_factor=0.5)

    try:
        # Get the total number of pages
        logger.info("Fetching total client page count...")
        response = session.get(
            'https://moja.superfaktura.sk/clients/index.json/listinfo:1', 
            headers=get_auth("superfaktura"),
            timeout=30
        )
        response.raise_for_status()
        page_count = response.json()["pageCount"]
        logger.info(f"Total client pages to fetch: {page_count}")

        pages = []
        for p in range(page_count):
            page_num = p + 1
            try:
                # Add small delay to avoid overwhelming API
                time.sleep(0.2)

                logger.info(f"Fetching client page {page_num}/{page_count}")
                response = session.get(
                    f'https://moja.superfaktura.sk/clients/index.json/page:{page_num}', 
                    headers=get_auth("superfaktura"),
                    timeout=30
                )
                response.raise_for_status()
                page_data = response.json()

                if not page_data:
                    logger.warning(f"Empty response for client page {page_num}")
                    continue

                pages.extend(page_data)

                # Log progress
                if page_num % 10 == 0 or page_num == page_count:
                    logger.info(f"Completed {page_num}/{page_count} client pages")

            except Exception as e:
                logger.error(f"Error fetching client page {page_num}: {str(e)}")
                # Continue with next page instead of failing the entire process
                continue

        # Save all client data to JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(pages, json_file, indent=2)

        logger.info(f"Client data has been successfully written to {json_file_path} with page_count: {page_count}")
        print(f'Data has been successfully written to {json_file_path} with page_count: {page_count}')

    except Exception as e:
        logger.error(f"Failed to fetch client data: {str(e)}", exc_info=True)
        print(f"ERROR: Failed to fetch client data: {str(e)}")
        raise

def get_superfaktura_clients_all(run_date, data_path):
    """
    Processes and saves Superfaktura client data for a given run date.
    Args:
        run_date (datetime): The date for which the data is being processed.
        data_path (str): The base path where the data is stored and will be saved.
    Raises:
        FileNotFoundError: If the JSON file containing the client data does not exist.
        json.JSONDecodeError: If the JSON file cannot be decoded.
        subprocess.CalledProcessError: If there is an error creating the directories.
        Exception: For any other errors that occur during processing.
    Side Effects:
        Creates directories for saving processed data.
        Writes processed client data to parquet files.
        Prints the shapes of the saved data.
    Example:
        >>> from datetime import datetime
        >>> run_date = datetime.strptime('2023-10-11', '%Y-%m-%d')
        >>> data_path = '/path/to/data'
        >>> get_superfaktura_clients_all(run_date, data_path)
    """
    run_date_key = run_date.strftime('%Y%m%d')
    json_file_path = f"{data_path}/landing/client_all/{run_date_key}/client_all_raw.json"
    save_path_client_all = f"{data_path}/preprocess/client_all/{run_date_key}"
    save_path_clientstat_all = f"{data_path}/preprocess/clientstat_all/{run_date_key}"

    with open(json_file_path, 'r') as json_file:
        pages = json.load(json_file)

    superfaktura_clients_client = [x["Client"] for x in pages]
    superfaktura_clients_clientstat = [x["ClientStat"] for x in pages]

    # Use ensure_directory instead of subprocess
    ensure_directory(save_path_client_all)
    ensure_directory(save_path_clientstat_all)

    superfaktura_clients_client_schema = _get_schema_from_json(superfaktura_clients_client)
    superfaktura_clients_clientstat_schema = _get_schema_from_json(superfaktura_clients_clientstat)

    # Save to BigQuery compatible format
    client_df = pl.from_dicts(superfaktura_clients_client, schema=superfaktura_clients_client_schema)
    client_df.write_parquet(f"{save_path_client_all}/client_all.parquet")

    clientstat_df = pl.from_dicts(superfaktura_clients_clientstat, schema=superfaktura_clients_clientstat_schema)
    clientstat_df.write_parquet(f"{save_path_clientstat_all}/clientstat_all.parquet")

    print(f'Data has been successfully written to {save_path_client_all} with shape: {client_df.shape}')
    print(f'Data has been successfully written to {save_path_clientstat_all} with shape: {clientstat_df.shape}')

def get_json_superfaktura_invoices_all(run_date, data_path):
    """
    Fetches all invoices from the SuperFaktura API and saves them as a JSON file.
    This version is designed to be more robust against network issues by using:
    - Automatic retries with exponential backoff
    - Batch processing and intermediate saving
    - Exception handling with detailed logging

    Args:
        run_date (datetime): The date of the run, used to create a unique directory for the output file.
        data_path (str): The base path where the JSON file will be saved.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request after all retries.
        IOError: If there is an issue writing the JSON file.
    Example:
        get_json_superfaktura_invoices_all(datetime.now(), '/path/to/data')
    """
    MAX_RETRIES = 5
    BATCH_SIZE = 50  # Process 50 pages at a time

    run_date_key = run_date.strftime('%Y%m%d')
    landing_dir = f"{data_path}/landing/invoices_all/{run_date_key}"
    ensure_directory(landing_dir)
    json_file_path = f"{landing_dir}/invoices_all_raw.json"

    # Create a robust session with automatic retries
    session = create_robust_session(retries=MAX_RETRIES, backoff_factor=1.0, 
                           status_forcelist=(429, 500, 502, 503, 504))

    try:
        # Get the total number of pages
        logger.info("Fetching total invoice page count...")
        response = session.get(
            'https://moja.superfaktura.sk/invoices/index.json/listinfo:1/type:regular|cancel|proforma|tax_document/created:0', 
            headers=get_auth("superfaktura"),
            timeout=30
        )
        response.raise_for_status()
        page_count = response.json()["pageCount"]
        logger.info(f"Total invoice pages to fetch: {page_count}")

        # Process in batches to avoid long-running requests
        all_pages = []

        for batch_start in range(0, page_count, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, page_count)
            logger.info(f"Processing invoice pages {batch_start+1} to {batch_end} of {page_count}...")

            batch_pages = []
            for p in range(batch_start, batch_end):
                page_num = p + 1
                try:
                    # Add jitter to avoid rate limiting
                    jitter = random.uniform(0.1, 0.5)
                    time.sleep(jitter)

                    logger.info(f"Fetching invoice page {page_num}/{page_count}")
                    response = session.get(
                        f'https://moja.superfaktura.sk/invoices/index.json/type:regular|cancel|proforma|tax_document/page:{page_num}/created:0', 
                        headers=get_auth("superfaktura"),
                        timeout=30
                    )
                    response.raise_for_status()
                    page_data = response.json()

                    if not page_data:
                        logger.warning(f"Empty response for page {page_num}")
                        continue

                    batch_pages.extend(page_data)

                    # Save progress after every 10 pages
                    if page_num % 10 == 0 or page_num == page_count:
                        logger.info(f"Completed {page_num}/{page_count} pages")

                except Exception as e:
                    logger.error(f"Error fetching page {page_num}: {str(e)}")
                    # Continue with next page instead of failing the entire process
                    continue

            # Add batch to all pages
            all_pages.extend(batch_pages)

            # Save intermediate results after each batch
            intermediate_file = f"{landing_dir}/invoices_all_raw_batch_{batch_start+1}_{batch_end}.json"
            with open(intermediate_file, 'w') as json_file:
                json.dump(batch_pages, json_file, indent=2)
            logger.info(f"Saved batch {batch_start+1}-{batch_end} to {intermediate_file}")

            # Add delay between batches to avoid overwhelming the API
            time.sleep(2)

        # Save the final combined result
        with open(json_file_path, 'w') as json_file:
            json.dump(all_pages, json_file, indent=2)
        logger.info(f"All invoice data has been successfully written to {json_file_path} with page_count: {page_count}")
        print(f'Data has been successfully written to {json_file_path} with page_count: {page_count}')

    except Exception as e:
        logger.error(f"Failed to fetch invoice data: {str(e)}", exc_info=True)
        print(f"ERROR: Failed to fetch invoice data: {str(e)}")
        raise

def get_superfaktura_invoices_all(run_date, data_path):
    """
    Processes Superfaktura invoices and invoice items from a JSON file and saves them as Parquet files.
    Args:
        run_date (datetime): The date for which the invoices are being processed. Used to generate file paths.
        data_path (str): The base path where the data is stored and where the processed files will be saved.
    Raises:
        FileNotFoundError: If the JSON file containing the invoices cannot be found.
        json.JSONDecodeError: If the JSON file cannot be decoded.
        subprocess.CalledProcessError: If there is an error creating the directories for saving the processed files.
    Side Effects:
        Creates directories for saving the processed invoice and invoice item data.
        Writes the processed invoice and invoice item data to Parquet files.
    Example:
        >>> from datetime import datetime
        >>> run_date = datetime.strptime('2023-10-11', '%Y-%m-%d')
        >>> data_path = '/path/to/data'
        >>> get_superfaktura_invoices_all(run_date, data_path)
        Data has been successfully written to /path/to/data/preprocess/invoices_all/20231011 with shape: (100, 10)
        Data has been successfully written to /path/to/data/preprocess/invoiceitem_all/20231011 with shape: (500, 15)
    """
    run_date_key = run_date.strftime('%Y%m%d')
    json_file_path = f"{data_path}/landing/invoices_all/{run_date_key}/invoices_all_raw.json"
    save_path_invoices_all = f"{data_path}/preprocess/invoices_all/{run_date_key}"
    save_path_invoiceitem_all = f"{data_path}/preprocess/invoiceitem_all/{run_date_key}"

    with open(json_file_path, 'r') as json_file:
        pages = json.load(json_file)

    superfaktura_invoices_invoice = []
    for x in pages:
        invoice = x["Invoice"]
        if "variable" in invoice and invoice["variable"] is None:
            invoice["variable"] = ""  # Replace null with empty string
        superfaktura_invoices_invoice.append(invoice)
    
    superfaktura_invoices_invoiceitem = [x2 for x1 in pages for x2 in x1["InvoiceItem"]]

    # Use ensure_directory instead of subprocess
    ensure_directory(save_path_invoices_all)
    ensure_directory(save_path_invoiceitem_all)

    superfaktura_invoices_invoice_schema = _get_schema_from_json(superfaktura_invoices_invoice)
    superfaktura_invoices_invoiceitem_schema = _get_schema_from_json(superfaktura_invoices_invoiceitem)

    # Save to BigQuery compatible format
    invoices_df = pl.from_dicts(superfaktura_invoices_invoice, schema=superfaktura_invoices_invoice_schema)
    invoices_df.write_parquet(f"{save_path_invoices_all}/invoices_all.parquet")

    invoiceitem_df = pl.from_dicts(superfaktura_invoices_invoiceitem, schema=superfaktura_invoices_invoiceitem_schema)
    invoiceitem_df.write_parquet(f"{save_path_invoiceitem_all}/invoiceitem_all.parquet")

    print(f'Data has been successfully written to {save_path_invoices_all} with shape: {invoices_df.shape}')
    print(f'Data has been successfully written to {save_path_invoiceitem_all} with shape: {invoiceitem_df.shape}')

def get_json_superfaktura_expenses_categories(run_date, data_path):
    """
    Fetches expense categories from the SuperFaktura API and saves them as a JSON file.
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
        response = json.load(json_file)

    expense_categories_flat = []

    def _category_dict(parent, child):
        return {
            "id": child.get("id", ""),
            "parent_id": parent.get("id", "") if parent else "",
            "parent_name": parent.get("name", "") if parent else "",
            "name": child.get("name", ""),
            "level": 1 if not parent else 2,
            "fullname": child.get("name", "") if not parent else f"{parent.get('name', '')} / {child.get('name', '')}"
        }

    if isinstance(response, dict) and "ExpenseCategory" in response:
        categories = response["ExpenseCategory"]
    elif isinstance(response, list):
        categories = response
    else:
        categories = []
        logger.warning("Unexpected expenses categories response format")

    for category in categories:
        expense_categories_flat.append(_category_dict(None, category))
        if isinstance(category, dict) and "children" in category and category["children"]:
            for child in category["children"]:
                expense_categories_flat.append(_category_dict(category, child))

    save_path = f"{data_path}/preprocess/expenses_categories"
    ensure_directory(save_path)

    expense_categories_df = pl.from_dicts(expense_categories_flat)
    expense_categories_df.write_parquet(f"{save_path}/expenses_categories.parquet")

    print(f'Data has been successfully written to {save_path} with shape: {expense_categories_df.shape}')

def get_json_superfaktura_expenses_all(run_date, data_path):
    """
    Fetches all expense data from the SuperFaktura API and saves it as a JSON file.
    Uses a robust session with retries for reliability.

    Args:
        run_date (datetime): The date when the function is run, used to format the run_date_key.
        data_path (str): The path where the JSON file will be saved.
    Returns:
        None
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request after all retries.
        IOError: If there is an issue writing the JSON file.
    Example:
        get_json_superfaktura_expenses_all(datetime.now(), '/path/to/data')
    """
    MAX_RETRIES = 5

    run_date_key = run_date.strftime('%Y%m%d')
    landing_dir = f"{data_path}/landing/expenses_all"
    ensure_directory(landing_dir)
    json_file_path = f"{landing_dir}/expenses_all_raw.json"

    # Create a robust session with automatic retries
    session = create_robust_session(retries=MAX_RETRIES, backoff_factor=0.5)

    try:
        # Get the total number of pages
        logger.info("Fetching total expenses page count...")
        response = session.get(
            'https://moja.superfaktura.sk/expenses/index.json/listinfo:1', 
            headers=get_auth("superfaktura"),
            timeout=30
        )
        response.raise_for_status()
        page_count = response.json()["pageCount"]
        logger.info(f"Total expenses pages to fetch: {page_count}")

        pages = []
        for p in range(page_count):
            page_num = p + 1
            try:
                # Add small delay to avoid overwhelming API
                time.sleep(0.2)

                logger.info(f"Fetching expenses page {page_num}/{page_count}")
                response = session.get(
                    f'https://moja.superfaktura.sk/expenses/index.json/page:{page_num}', 
                    headers=get_auth("superfaktura"),
                    timeout=30
                )
                response.raise_for_status()
                page_data = response.json()

                if not page_data:
                    logger.warning(f"Empty response for expenses page {page_num}")
                    continue

                pages.extend(page_data)

                # Log progress
                if page_num % 10 == 0 or page_num == page_count:
                    logger.info(f"Completed {page_num}/{page_count} expenses pages")

            except Exception as e:
                logger.error(f"Error fetching expenses page {page_num}: {str(e)}")
                # Continue with next page instead of failing the entire process
                continue

        # Save all expenses data to JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(pages, json_file, indent=2)

        logger.info(f"Expenses data has been successfully written to {json_file_path} with page_count: {page_count}")
        print(f'Data has been successfully written to {json_file_path} with page_count: {page_count}')

    except Exception as e:
        logger.error(f"Failed to fetch expenses data: {str(e)}", exc_info=True)
        print(f"ERROR: Failed to fetch expenses data: {str(e)}")
        raise

def get_superfaktura_expenses_all(run_date, data_path):
    """
    Processes Superfaktura expenses data and saves it in Parquet format.
    Args:
        run_date (datetime): The date when the function is run, used to generate a key.
        data_path (str): The base path where the data is stored and where the processed data will be saved.
    Raises:
        FileNotFoundError: If the JSON file containing the expenses data does not exist.
        json.JSONDecodeError: If the JSON file cannot be decoded.
        subprocess.CalledProcessError: If there is an error creating the directories.
    Side Effects:
        Creates directories for saving processed data if they do not exist.
        Writes processed expenses and expense items data to Parquet files.
    Example:
        >>> from datetime import datetime
        >>> get_superfaktura_expenses_all(datetime.now(), '/path/to/data')
    """
    json_file_path = f"{data_path}/landing/expenses_all/expenses_all_raw.json"
    save_path = f"{data_path}/preprocess/expenses_all"

    with open(json_file_path, 'r') as json_file:
        pages = json.load(json_file)

    superfaktura_expenses = [x["Expense"] for x in pages]

    ensure_directory(save_path)

    superfaktura_expenses_schema = _get_schema_from_json(superfaktura_expenses)
    expenses_df = pl.from_dicts(superfaktura_expenses, schema=superfaktura_expenses_schema)
    expenses_df.write_parquet(f"{save_path}/expenses_all.parquet")

    print(f'Data has been successfully written to {save_path} with shape: {expenses_df.shape}')

def main():
    """
    Main function to preprocess all Superfaktura data.
    This function performs the following steps:
    1. Sets the data path to the Superfaktura data lake.
    2. Gets the current date and time in the 'Europe/Bratislava' timezone.
    3. Calls functions to retrieve and process Superfaktura clients data.
    4. Calls functions to retrieve and process Superfaktura invoices data.
    5. Calls functions to retrieve and process Superfaktura expenses categories data.
    6. Calls functions to retrieve and process Superfaktura expenses data.
    The following functions are called:
    - get_json_superfaktura_clients_all(run_date, data_path)
    - get_superfaktura_clients_all(run_date, data_path)
    - get_json_superfaktura_invoices_all(run_date, data_path)
    - get_superfaktura_invoices_all(run_date, data_path)
    - get_json_superfaktura_expenses_categories(run_date, data_path)
    - get_superfaktura_expenses_categories(run_date, data_path)
    - get_json_superfaktura_expenses_all(run_date, data_path)
    - get_superfaktura_expenses_all(run_date, data_path)
    """
    # Import config from main module to get current settings
    import config
    data_path = config.BASE_DATA_PATH  # Use path from config
    run_date = datetime.now(timezone('Europe/Bratislava'))

    get_json_superfaktura_clients_all(run_date, data_path)
    get_superfaktura_clients_all(run_date, data_path)

    get_json_superfaktura_invoices_all(run_date, data_path)
    get_superfaktura_invoices_all(run_date, data_path)

    get_json_superfaktura_expenses_categories(run_date, data_path)
    get_superfaktura_expenses_categories(run_date, data_path)

    get_json_superfaktura_expenses_all(run_date, data_path)
    get_superfaktura_expenses_all(run_date, data_path)

if __name__ == "__main__":
    main()