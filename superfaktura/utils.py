"""
Utility functions for the Superfaktura ETL pipeline.
"""
import os
import logging
from datetime import datetime

import polars as pl

logger = logging.getLogger(__name__)

def ensure_directory(directory_path):
    """
    Ensures that the specified directory exists.
    
    Args:
        directory_path (str): Path to the directory
        
    Returns:
        str: The directory path
    """
    try:
        os.makedirs(directory_path, exist_ok=True)
        logger.debug(f"Directory ensured: {directory_path}")
    except Exception as e:
        logger.error(f"Error ensuring directory {directory_path}: {str(e)}")
        raise
    
    return directory_path

def save_dataframe(df, file_path, format="parquet"):
    """
    Saves a Polars DataFrame to a file.
    
    Args:
        df (pl.DataFrame): The DataFrame to save
        file_path (str): The path where the file will be saved
        format (str): The format to save as (parquet, csv, json)
        
    Returns:
        str: The file path
    """
    # Ensure the directory exists
    directory = os.path.dirname(file_path)
    ensure_directory(directory)
    
    try:
        if format.lower() == "parquet":
            df.write_parquet(file_path)
        elif format.lower() == "csv":
            df.write_csv(file_path)
        elif format.lower() == "json":
            df.write_json(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.debug(f"DataFrame saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving DataFrame to {file_path}: {str(e)}")
        raise
    
    return file_path

def load_dataframe(file_path, format="parquet"):
    """
    Loads a DataFrame from a file.
    
    Args:
        file_path (str): The path to the file
        format (str): The format of the file (parquet, csv, json)
        
    Returns:
        pl.DataFrame: The loaded DataFrame
    """
    try:
        if format.lower() == "parquet":
            df = pl.read_parquet(file_path)
        elif format.lower() == "csv":
            df = pl.read_csv(file_path)
        elif format.lower() == "json":
            df = pl.read_json(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.debug(f"DataFrame loaded from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading DataFrame from {file_path}: {str(e)}")
        raise

def get_timestamp():
    """
    Returns a formatted timestamp string.
    
    Returns:
        str: Formatted timestamp
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")