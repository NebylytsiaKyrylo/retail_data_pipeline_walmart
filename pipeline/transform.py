import pandas as pd
import logging

logger = logging.getLogger(__name__)


def transform(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw sales data into a structured format suitable for further analysis.

    This function takes a DataFrame containing raw sales data, applies several transformations
    and filtering rules, and returns a cleaned and processed DataFrame as output. It works by
    performing operations such as type conversions, filtering, imputation of missing data, and
    schema enforcement.

    Args:
        raw_data (pd.DataFrame): Unprocessed sales data containing columns such as "Date",
            "Store_ID", "Dept", "IsHoliday", "Weekly_Sales", "CPI", and "Unemployment".

    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame ready for analysis or modeling.
            This DataFrame contains columns restricted to "Store_ID", "Month", "Dept",
            "IsHoliday", "Weekly_Sales", "CPI", and "Unemployment", with missing numeric
            values imputed using the mean and non-performing weekly sales (< 10000) filtered out.
    """

    # Create a deep copy to avoid SettingWithCopyWarning
    df = raw_data.copy()

    try:
        if df.empty:
            logger.warning("DataFrame is empty")
            return pd.DataFrame()

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
        initial_count = len(df)
        df = df[df['Weekly_Sales'] > 10000]

        logger.info(f"Data transformed correctly: {initial_count} -> {len(df)} rows after filtering")

    except Exception as e:
        # Log any error that occurs during the process
        logger.error(f"An error occurred while transforming data: {e}", exc_info=True)
        return pd.DataFrame()

    return df
