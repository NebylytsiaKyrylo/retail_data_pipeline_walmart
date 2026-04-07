import logging

import pandas as pd

logger = logging.getLogger(__name__)


def aggregate(clean_data: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the monthly mean of weekly sales from the given clean data.

    This function takes a pandas DataFrame containing at least "Month" and
    "Weekly_Sales" columns, and calculates the mean of weekly sales for each
    month. The resulting aggregated data is rounded to two decimal places.

    If the input DataFrame is empty, the function logs a warning and returns
    an empty DataFrame. In the case of an exception, the error is logged, and
    an empty DataFrame is returned.

    Args:
        clean_data (pd.DataFrame): A pandas DataFrame containing at least two
            columns: "Month" (categorical) and "Weekly_Sales" (numeric). The
            input data should not have missing or invalid values.

    Returns:
        pd.DataFrame: A DataFrame containing the aggregated mean values of
        weekly sales for each month, with two columns:
            - "Month": The unique values from the input "Month" column.
            - "Weekly_Sales": The mean of "Weekly_Sales" values for each
              month, rounded to two decimal places. If the input is empty or
              an error occurs, returns an empty DataFrame.
    """
    try:
        if clean_data.empty:
            logger.warning("DataFrame is empty")
            return pd.DataFrame()

        agg_data = (
            clean_data[['Month', 'Weekly_Sales']]
            .groupby('Month', as_index=False)
            .agg(Weekly_Sales=('Weekly_Sales', 'mean'))
            .round(2)
        )

    except Exception as e:
        logger.error(f"An error occurred while aggregating data: {e}", exc_info=True)
        return pd.DataFrame()

    return agg_data
