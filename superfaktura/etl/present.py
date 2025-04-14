import polars as pl
from polars import col
import os


def present_client(data_path):
    """
    Processes and combines client data from parquet files, then saves the combined data to a specified directory.
    Args:
        data_path (str): The base directory path where the client data is stored and where the processed data will be saved.
    The function performs the following steps:
    1. Reads the 'client_all' data from a parquet file located at 'data_path/preprocess/client_all/20240930/client_all.parquet'.
    2. Reads the 'client_increment' data from parquet files located at 'data_path/preprocess/client_increment/*/client_increment.parquet'.
    3. Combines the 'client_all' and 'client_increment' data, ensuring uniqueness.
    4. Saves the combined data to 'data_path/present/client/client.parquet', creating the directory if it does not exist.
    """
    import os
    if not os.path.exists(f"{data_path}/preprocess/client_all/20250310/client_all.parquet"):
        print(f"Warning: File {data_path}/preprocess/client_all/20250310/client_all.parquet does not exist. Creating empty dataframe.")
        client_all_import = pl.DataFrame({"id": [], "name": [], "ico": [], "dic": [], "ic_dph": []})
    else:
        parquet_file_path_all = data_path + '/preprocess/client_all/20250310/client_all.parquet'
        client_all_import = pl.read_parquet(parquet_file_path_all)
    
    # Check if increment files exist
    if not os.path.exists(f"{data_path}/preprocess/client_increment/client_increment.parquet"):
        print(f"Warning: No client_increment files found. Using only client_all data.")
        client_all_load = client_all_import
    else:
        parquet_file_path_increment = data_path + "/preprocess/client_increment/client_increment.parquet"
        client_increment_import = pl.read_parquet(parquet_file_path_increment).unique()
        client_all_load = pl.concat([client_all_import, client_increment_import]).unique()

    save_path = f"{data_path}/present/client"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/client.parquet"
    client_all_load.write_parquet(parquet_file_path)

def present_clientstat(data_path):
    """
    Processes and combines client statistics data from multiple parquet files, 
    then saves the combined data to a specified directory.
    Args:
        data_path (str): The base directory path where the client statistics data is stored.
    Steps:
        1. Reads the full client statistics data from a parquet file.
        2. Reads the incremental client statistics data from multiple parquet files and removes duplicates.
        3. Combines the full and incremental client statistics data and removes duplicates.
        4. Creates a directory for saving the combined data if it does not exist.
        5. Saves the combined client statistics data to a parquet file in the specified directory.
    """
    import os
    if not os.path.exists(f"{data_path}/preprocess/clientstat_all/20250310/clientstat_all.parquet"):
        print(f"Warning: File {data_path}/preprocess/clientstat_all/20250310/clientstat_all.parquet does not exist. Creating empty dataframe.")
        clientstat_all_import = pl.DataFrame({"client_id": [], "total_amount": [], "invoices_count": []})
    else:
        parquet_file_path_all = data_path + '/preprocess/clientstat_all/20250310/clientstat_all.parquet'
        clientstat_all_import = pl.read_parquet(parquet_file_path_all)
    
    # Check if increment files exist
    if not os.path.exists(f"{data_path}/preprocess/clientstat_increment/clientstat_increment.parquet"):
        print(f"Warning: No clientstat_increment files found. Using only clientstat_all data.")
        clientstat_all_load = clientstat_all_import
    else:
        parquet_file_path_increment = data_path + "/preprocess/clientstat_increment/clientstat_increment.parquet"
        clientstat_increment_import = pl.read_parquet(parquet_file_path_increment).unique()
        clientstat_all_load = pl.concat([clientstat_all_import, clientstat_increment_import]).unique()

    save_path = f"{data_path}/present/clientstat"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/clientstat.parquet"
    clientstat_all_load.write_parquet(parquet_file_path)

def _init_client_tmp(data_path):
    """
    Initializes and processes client data from parquet files.
    This function reads client data from two parquet files: one containing all client data
    and another containing incremental client data. It concatenates these datasets, removes
    duplicate entries, and filters the resulting dataset to include only clients with non-empty
    'ico', 'dic', or 'ic_dph' fields. Finally, it selects and renames specific columns for the
    output.
    Args:
        data_path (str): The base path to the directory containing the parquet files.
    Returns:
        polars.DataFrame: A DataFrame containing the processed client data with columns
        'client_id', 'ico', 'dic', and 'ic_dph'.
    """
    import os
    if not os.path.exists(f"{data_path}/preprocess/client_all/20250310/client_all.parquet"):
        print(f"Warning: File {data_path}/preprocess/client_all/20250310/client_all.parquet does not exist. Creating empty dataframe.")
        # Create a minimal dataframe with the required columns
        client_all_load = pl.DataFrame({
            "id": [], 
            "name": [], 
            "ico": [], 
            "dic": [], 
            "ic_dph": []
        })
    else:
        # Try to load the parquet files
        try:
            parquet_file_path_all = data_path + '/preprocess/client_all/20250310/client_all.parquet'
            client_all_import = pl.read_parquet(parquet_file_path_all)
            
            # Check if increment files exist
            if os.path.exists(f"{data_path}/preprocess/client_increment/client_increment.parquet"):
                parquet_file_path_increment = data_path + "/preprocess/client_increment/client_increment.parquet"
                client_increment_import = pl.read_parquet(parquet_file_path_increment).unique()
                client_all_load = pl.concat([client_all_import, client_increment_import]).unique()
            else:
                print(f"Warning: No client_increment files found. Using only client_all data.")
                client_all_load = client_all_import
        except Exception as e:
            print(f"Error reading client parquet files: {e}")
            # Create an empty dataframe with necessary columns
            client_all_load = pl.DataFrame({
                "id": [], 
                "name": [], 
                "ico": [], 
                "dic": [], 
                "ic_dph": []
            })

    # Only filter if there are rows and the required columns exist
    if len(client_all_load) > 0 and all(col in client_all_load.columns for col in ['ico', 'dic', 'ic_dph', 'id']):
        client_tmp = client_all_load.filter(
            (pl.col('ico') != "") | (pl.col('dic') != "") | (pl.col('ic_dph') != "")
        ).select(
            pl.col('id').alias('client_id'),
            'ico', 'dic', 'ic_dph'
        )
    else:
        # Return an empty dataframe with the right structure
        client_tmp = pl.DataFrame({
            "client_id": [], 
            "ico": [], 
            "dic": [], 
            "ic_dph": []
        })
        
    return client_tmp

def present_invoices(data_path):
    """
    Processes and presents invoices by reading, merging, and transforming data from parquet files.
    Args:
        data_path (str): The base directory path where the data files are located.
    Steps:
        1. Reads the complete set of invoices from a parquet file.
        2. Reads the incremental set of invoices from parquet files and ensures uniqueness.
        3. Concatenates the complete and incremental invoices, ensuring uniqueness.
        4. Initializes temporary client data.
        5. Joins the invoices data with the client data on 'client_id'.
        6. Strips whitespace from the 'variable' column and renames it.
        7. Classifies invoices into segments ('B2C', 'B2B', 'other') based on various conditions.
        8. Identifies crowdfunding invoices based on the 'variable' column.
        9. Determines the 'master_id' based on the presence of 'order_no' or 'variable'.
        10. Saves the processed invoices to a parquet file in the specified directory.
    Returns:
        None
    """
    import os
    if not os.path.exists(f"{data_path}/preprocess/invoices_all/20250310/invoices_all.parquet"):
        print(f"Warning: File {data_path}/preprocess/invoices_all/20250310/invoices_all.parquet does not exist. Creating empty dataframe.")
        # Create empty dataframe with required columns
        invoices_all_import = pl.DataFrame({
            "id": [], 
            "client_id": [], 
            "variable": [], 
            "order_no": [], 
            "invoice_no_formatted": [], 
            "invoice_no_formatted_raw": []
        })
    else:
        parquet_file_path_all = data_path + '/preprocess/invoices_all/20250310/invoices_all.parquet'
        invoices_all_import = pl.read_parquet(parquet_file_path_all)
    
    # Check if increment files exist
    if not os.path.exists(f"{data_path}/preprocess/invoices_increment/invoices_increment.parquet"):
        print(f"Warning: No invoices_increment files found. Using only invoices_all data.")
        invoices_all_load = invoices_all_import
    else:
        parquet_file_path_increment = data_path + "/preprocess/invoices_increment/invoices_increment.parquet"
        invoices_increment_import = pl.read_parquet(parquet_file_path_increment).unique()
        invoices_all_load = pl.concat([invoices_all_import, invoices_increment_import]).unique()

    client_tmp = _init_client_tmp(data_path)

    invoices_all_load = \
        invoices_all_load \
            .join(client_tmp,
                 'client_id',
                 'left')\
            .with_columns([
                pl.col('variable').fill_null("").cast(pl.Utf8).str.strip_chars().alias('variable')
            ]) \
            .with_columns(pl
                          .when(pl.col('variable') == pl.col('order_no')).then(pl.lit("B2C")) \
                          .when(pl.col('variable') == pl.col('invoice_no_formatted_raw')).then(pl.lit("B2B")) \
                          .when(pl.col('variable') == pl.col('invoice_no_formatted')).then(pl.lit("B2B")) \
                          .when((pl.col('ico').is_not_null()) | (pl.col('ico')!="")).then(pl.lit("B2B")) \
                          .when((pl.col('dic').is_not_null()) | (pl.col('ico')!="")).then(pl.lit("B2B")) \
                          .when((pl.col('ic_dph').is_not_null()) | (pl.col('ico')!="")).then(pl.lit("B2B")) \
                          .otherwise(pl.lit('other')).alias('segment')) \
            .with_columns(pl.col("variable").str.starts_with(pl.lit('122')).alias("crowdfunding")) \
            .with_columns(pl.when(pl.col('order_no').is_not_null()).then(pl.col('order_no')) \
                          .otherwise(pl.col('variable')).alias('master_id'))

    save_path = f"{data_path}/present/invoices"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/invoices.parquet"
    invoices_all_load.write_parquet(parquet_file_path)

def present_invoiceitem(data_path):
    """
    Processes and combines invoice item data from parquet files, then saves the result to a specified directory.
    Args:
        data_path (str): The base directory path where the input and output data are stored.
    Steps:
        1. Reads the 'invoiceitem_all' parquet file from the specified path.
        2. Drops the 'AccountingDetail' column and casts discount-related columns to Float64.
        3. Reads the 'invoiceitem_increment' parquet files from the specified path.
        4. Drops the 'AccountingDetail' column and casts discount-related columns to Float64.
        5. Combines the 'invoiceitem_all' and 'invoiceitem_increment' data, ensuring uniqueness.
        6. Saves the combined data to a new parquet file in the 'present/invoiceitem' directory.
    Returns:
        None
    """
    import os
    # Function to conditionally drop 'AccountingDetail' if it exists
    def drop_accounting_detail(df):
        if 'AccountingDetail' in df.columns:
            return df.drop('AccountingDetail')
        return df
    
    def process_discount_columns(df):
        """Process discount columns in a dataframe, ensuring they exist before casting"""
        cols_to_cast = []
        for col_name in ['discount', 'discount_no_vat', 'discount_with_vat', 
                        'discount_with_vat_total', 'discount_no_vat_total']:
            if col_name in df.columns:
                cols_to_cast.append(pl.col(col_name).cast(pl.Float64))
        
        if cols_to_cast:
            return df.with_columns(cols_to_cast)
        return df

    # Check for invoiceitem_all file
    if not os.path.exists(f"{data_path}/preprocess/invoiceitem_all/20250310/invoiceitem_all.parquet"):
        print(f"Warning: File {data_path}/preprocess/invoiceitem_all/20250310/invoiceitem_all.parquet does not exist. Creating empty dataframe.")
        # Create empty dataframe with necessary columns
        invoiceitem_all_import = pl.DataFrame({
            "id": [], 
            "invoice_id": [], 
            "name": [], 
            "description": [], 
            "discount": [], 
            "discount_no_vat": [], 
            "discount_with_vat": [],
            "discount_with_vat_total": [],
            "discount_no_vat_total": []
        })
    else:
        # Load and process 'invoiceitem_all' file
        parquet_file_path_all = data_path + '/preprocess/invoiceitem_all/20250310/invoiceitem_all.parquet'
        invoiceitem_all_import = pl.read_parquet(parquet_file_path_all).pipe(drop_accounting_detail)
        invoiceitem_all_import = process_discount_columns(invoiceitem_all_import)

    # Check for invoiceitem_increment files
    if not os.path.exists(f"{data_path}/preprocess/invoiceitem_increment/invoiceitem_increment.parquet"):
        print(f"Warning: No invoiceitem_increment files found. Using only invoiceitem_all data.")
        invoiceitem_all_load = invoiceitem_all_import
    else:
        # Load and process 'invoiceitem_increment' files
        parquet_file_path_increment = data_path + "/preprocess/invoiceitem_increment/invoiceitem_increment.parquet"
        invoiceitem_increment_import = pl.read_parquet(parquet_file_path_increment).pipe(drop_accounting_detail)
        invoiceitem_increment_import = process_discount_columns(invoiceitem_increment_import).unique()
        
        # Ensure all columns have compatible types
        for col in invoiceitem_all_import.columns:
            if col in invoiceitem_increment_import.columns:
                if invoiceitem_all_import[col].dtype != invoiceitem_increment_import[col].dtype:
                    # Cast to the type of the first DataFrame for consistency
                    invoiceitem_increment_import = invoiceitem_increment_import.with_columns(
                        pl.col(col).cast(invoiceitem_all_import[col].dtype)
                    )
        
        # Combine and ensure uniqueness
        invoiceitem_all_load = pl.concat([invoiceitem_all_import, invoiceitem_increment_import]).unique()
    
    # Save the processed data
    save_path = f"{data_path}/present/invoiceitem"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/invoiceitem.parquet"
    invoiceitem_all_load.write_parquet(parquet_file_path)


def present_expenses(data_path):
    """
    Processes and presents expenses data by reading from parquet files, transforming the data, 
    and saving the result to a new parquet file.
    Args:
        data_path (str): The base directory path where the data files are located.
    Returns:
        None
    The function performs the following steps:
    1. Constructs file paths for the expenses categories and all expenses parquet files.
    2. Reads the expenses categories and all expenses data from the respective parquet files.
    3. Drops unnecessary columns from the all expenses data if they exist.
    4. Selects and casts specific columns from the all expenses data.
    5. Joins the all expenses data with the expenses categories data on the 'expense_category_id' column.
    6. Creates a directory for saving the processed data if it does not already exist.
    7. Writes the processed expenses data to a new parquet file in the specified save path.
    """
    import os
    parquet_file_path_categories = f"{data_path}/preprocess/expenses_categories/expenses_categories.parquet"
    parquet_file_path_all = f"{data_path}/preprocess/expenses_all/expenses_all.parquet"
    
    # Check if files exist
    if not os.path.exists(parquet_file_path_categories):
        print(f"Warning: File {parquet_file_path_categories} does not exist. Creating empty expenses categories dataframe.")
        expenses_categories = pl.DataFrame({
            "id": [],
            "parent_id": [],
            "parent_name": [],
            "name": [],
            "level": [],
            "fullname": []
        })
    else:
        expenses_categories = pl.read_parquet(parquet_file_path_categories)
    
    if not os.path.exists(parquet_file_path_all):
        print(f"Warning: File {parquet_file_path_all} does not exist. Creating empty expenses dataframe.")
        expenses_df = pl.DataFrame({
            "id": [],
            "name": [],
            "type": [],
            "created": [],
            "due": [],
            "currency": [],
            "total": [],
            "expense_category_id": []
        })
    else:
        expenses_df = pl.read_parquet(parquet_file_path_all)
    
    # Remove columns if they exist
    columns_to_drop = ['rates', 'client_data', 'my_data']
    expenses_all_import = expenses_df.select([col for col in expenses_df.columns if col not in columns_to_drop])

    # Dynamically select only columns that exist in expenses_all_import
    available_columns = expenses_all_import.columns
    
    # Define columns to select and cast if they exist
    # Essential columns that should be in the final DataFrame
    essential_columns = [
        'id', 'name', 'type', 'created', 'due', 'currency', 'total'
    ]
    
    # Columns to cast to Float64 if they exist
    float_columns = [
        'paid', 'paid_vat', 'amount_paid', 'amount_paid_vat', 'amount',
        'amount_home', 'amount_country_home', 'vat', 'total', 'total_home',
        'total_country_home', 'discount', 'discount_total'
    ]
    
    # Optional columns to include if they exist
    optional_columns = [
        'user_id', 'user_profile_id', 'client_id', 'expense_category_id', 
        'sequence_id', 'number', 'variable', 'delivery', 'paydate', 'status',
        'tax', 'home_currency', 'comment', 'modified', 'recurring', 'taxdate',
        'accounting_date', 'document_number', 'expense_no', 'version', 'flag',
        'missing_bank_account', 'is_payable_by_tatrabanka'
    ]
    
    # Build list of select operations
    select_ops = []
    
    # First add essential and optional columns that exist
    for col_name in essential_columns + optional_columns:
        if col_name in available_columns:
            if col_name in float_columns:
                select_ops.append(col(col_name).cast(pl.Float64).alias(col_name))
            else:
                select_ops.append(col_name)
    
    # Select the columns
    expenses_all = expenses_all_import.select(select_ops)
    
    # Try joining with expenses_categories if expense_category_id exists
    if 'expense_category_id' in expenses_all.columns and len(expenses_categories) > 0:
        print("Joining with expense categories")
        # The join column in expenses_categories is 'id', not 'expense_category_id'
        expenses_all = expenses_all.join(
            expenses_categories,
            left_on='expense_category_id',
            right_on='id',
            how='left'
        )
    else:
        print("Skipping join with expense categories - column not found or categories empty")
    
    save_path = f"{data_path}/present/expenses"
    os.makedirs(save_path, exist_ok=True)
    parquet_file_path = f"{save_path}/expenses.parquet"
    expenses_all.write_parquet(parquet_file_path)

def main():
    """
    Main function to present various client and invoice data.
    This function sets the data path and calls several functions to present
    client information, client statistics, invoices, invoice items, and expenses.
    Functions called:
    - present_client(data_path)
    - present_clientstat(data_path)
    - present_invoices(data_path)
    - present_invoiceitem(data_path)
    - present_expenses(data_path)
    Args:
        None
    Returns:
        None
    """
    # Use the configured data path from config module
    import config
    data_path = config.BASE_DATA_PATH

    present_client(data_path)
    present_clientstat(data_path)

    present_invoices(data_path)
    present_invoiceitem(data_path)

    present_expenses(data_path)


if __name__ == "__main__":
    main()
