import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os

from config import DB_CONFIG

logger = logging.getLogger(__name__)


def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy database engine for a PostgreSQL database.

    This function generates a database connection string using the credentials
    and connection details defined in the application configuration settings. It
    then creates and returns a SQLAlchemy database engine object for interacting
    with the specified PostgreSQL database.

    Returns:
        Engine: An instance of SQLAlchemy Engine configured for the specified
            PostgreSQL database.

    """
    db_url = f"postgresql://{DB_CONFIG['db_user']}:{DB_CONFIG['db_password']}@{DB_CONFIG['db_host']}:{DB_CONFIG['db_port']}/{DB_CONFIG['db_name']}"
    return create_engine(db_url)


def extract(db_engine: Engine, parquet_file_path: str) -> pd.DataFrame:
    """
    Extracts and merges data from SQL and Parquet sources.

    Args:
        db_engine (Engine): The SQLAlchemy connection engine.
        parquet_file_path (str): Path to the extra data file.

    Returns:
        pd.DataFrame: The merged dataset.
    """
    try:
        if not os.path.exists(parquet_file_path):
            logger.error(f"Parquet file not found: {parquet_file_path}")
            return pd.DataFrame()

        # Define the SQL query
        sql_query = "SELECT * FROM walmart_sales.grocery_sales"

        # Load SQL data into a DataFrame
        df_sales = pd.read_sql(sql_query, db_engine)

        # Load Parquet data
        df_extra_data = pd.read_parquet(parquet_file_path, engine="pyarrow")

        # Check if both datasets are not empty
        if not df_sales.empty and not df_extra_data.empty:
            # Merge datasets on the index column
            extracted_df = pd.merge(df_sales, df_extra_data, how="inner", on="index")
            logger.info(f"Data extracted and {len(extracted_df)} rows merged correctly")
        else:
            logger.warning("No data found in the SQL or Parquet sources")
            return pd.DataFrame()

    except Exception as e:
        # Log any error that occurs during the process
        logger.error(f"An error occurred while extracting data: {e}", exc_info=True)
        return pd.DataFrame()

    return extracted_df
