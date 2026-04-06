import pandas as pd
from pandas import read_parquet
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import logging
from dotenv import load_dotenv
import os

# Initialize environment variables and logging
load_dotenv()
logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.INFO)

# Database credentials
db_user = os.environ.get("POSTGRES_USER")
db_password = os.environ.get("POSTGRES_PASSWORD")
db_name = os.environ.get("POSTGRES_DB")
db_host = os.environ.get("POSTGRES_HOST")
db_port = os.environ.get("POSTGRES_PORT")

db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
db_engine = create_engine(db_url)


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
        # Define the SQL query
        sql_query = "SELECT * FROM walmart_sales.grocery_sales"

        # Load SQL data into a DataFrame
        df_sales = pd.read_sql(sql_query, db_engine)

        # Load Parquet data
        df_extra_data = read_parquet(parquet_file_path, engine="pyarrow")

        # Check if both datasets are not empty
        if not df_sales.empty and not df_extra_data.empty:
            # Merge datasets on the index column
            extracted_df = pd.merge(df_sales, df_extra_data, how="inner", on="index")
            logging.info("Data extracted and merged correctly")
        else:
            logging.warning("No data found in the SQL or Parquet sources")
            return pd.DataFrame()

    except Exception as e:
        # Log any error that occurs during the process
        logging.error(f"An error occurred while extracting data: {e}", exc_info=True)
        return pd.DataFrame()

    return extracted_df


extracted_df = extract(db_engine, "raw_data/extra_data.parquet")


def transform(raw_data: pd.DataFrame) -> pd.DataFrame:
    # Create a deep copy to avoid SettingWithCopyWarning
    df = raw_data.copy()

    try:
        # Check if dataset is not empty
        if not df.empty:
            # Convert Date column to datetime format and extract month astype Int64 for avoided NaN values
            df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
            df['Month'] = (df['Date'].dt.month).astype("Int64")

            # Schema enforcement: drop unnecessary columns
            keep_cols = ["Store_ID", "Month", "Dept", "IsHoliday", "Weekly_Sales", "CPI", "Unemployment"]
            df = df[keep_cols]

            # Impute missing numerical values using the mean
            numeric_cols = ['Weekly_Sales', 'CPI', 'Unemployment']
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean()).round(2)

            # Keep only high-performing weeks > 10000
            df = df[df['Weekly_Sales'] > 10000]

            logging.info('Data transformed correctly')
        else:
            logging.warning("DataFrame is empty")
            return pd.DataFrame()

    except Exception as e:
        # Log any error that occurs during the process
        logging.error(f"An error occurred while transforming data: {e}", exc_info=True)
        return pd.DataFrame()

    return df


clean_data = transform(extracted_df)


def avg_weekly_sales_per_month(clean_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the average weekly sales for each month.
    """
    try:
        if not clean_data.empty:
            agg_data = (
                clean_data[['Month', 'Weekly_Sales']]
                .groupby('Month', as_index=False)
                .agg(Weekly_Sales=('Weekly_Sales', 'mean'))
                .round(2)
            )
        else:
            logging.warning("DataFrame is empty")
            return pd.DataFrame()
    except Exception as e:
        # Log any error that occurs during the process
        logging.error(f"An error occurred while aggregating data: {e}", exc_info=True)
        return pd.DataFrame()

    return agg_data


agg_data = avg_weekly_sales_per_month(clean_data)


def load(clean_data_df: pd.DataFrame, clean_data_file_path: str, agg_data_df: pd.DataFrame,
         agg_data_file_path: str) -> None:
    """
    Load the data to .csv files
    """
    try:
        if not clean_data_df.empty and not agg_data_df.empty:
            clean_data_df.to_csv(clean_data_file_path, index=False)
            agg_data_df.to_csv(agg_data_file_path, index=False)
            logging.info("Data loaded to .csv files successfully")
        else:
            logging.warning("DataFrame is empty")
    except Exception as e:
        # Log any error that occurs during the process
        logging.error(f"An error occurred while loading data: {e}", exc_info=True)


load(clean_data, "data/clean_data.csv", agg_data, "data/agg_data.csv")


def validation(file_path):
    try:
        file_exists = os.path.exists(file_path)
        logging.info(f'{file_path} exists')
        if not file_exists:
            logging.error('An error occurred while validating files')
            raise Exception(f"There is no file at the path {file_path}")
    except FileNotFoundError as e:
        logging.error(f'An error occurred while validating files: {e}')


validation("data/clean_data.csv")
validation("data/agg_data.csv")
