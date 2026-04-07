import logging
import os

import pandas as pd

from config import DATA_PATHS

logger = logging.getLogger(__name__)


def load(clean_data_df: pd.DataFrame, agg_data_df: pd.DataFrame) -> None:
    """
    Saves supplied DataFrame objects to respective CSV files. This function checks if the
    DataFrames are not empty before saving them and logs the operation's status.

    Args:
        clean_data_df (pd.DataFrame): Clean data DataFrame to be saved.
        clean_data_file_path (str): File path where the clean data CSV file will be saved.
        agg_data_df (pd.DataFrame): Aggregated data DataFrame to be saved.
        agg_data_file_path (str): File path where the aggregated data CSV file will be saved.
    """
    try:
        if clean_data_df.empty and agg_data_df.empty:
            logger.warning("Cannot load empty DataFrames")

        os.makedirs(DATA_PATHS['data_output'], exist_ok=True)

        clean_data_df.to_csv(DATA_PATHS['clean_output'], index=False)
        agg_data_df.to_csv(DATA_PATHS['agg_output'], index=False)

        logger.info("Data loaded to .csv files successfully")

    except Exception as e:
        # Log any error that occurs during the process
        logger.error(f"An error occurred while loading data: {e}", exc_info=True)
