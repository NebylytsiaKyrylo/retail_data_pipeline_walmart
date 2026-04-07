import pandas as pd
import pytest

from pipeline.transform import transform


def make_raw_df(**kwargs):
    """Build a minimal valid raw DataFrame."""
    data = {
        "index": [1, 2, 3],
        "Date": ["2022-01-07", "2022-02-04", "2022-03-04"],
        "Store_ID": [1, 1, 2],
        "Dept": [1, 2, 1],
        "IsHoliday": [False, True, False],
        "Weekly_Sales": [15000.0, 9000.0, 20000.0],
        "CPI": [211.1, 212.5, 213.0],
        "Unemployment": [8.1, 8.0, 7.9],
    }
    data.update(kwargs)
    return pd.DataFrame(data)


class TestTransformHappyPath:
    def test_returns_dataframe(self):
        # Verify that transform returns a pandas DataFrame object
        result = transform(make_raw_df())
        assert isinstance(result, pd.DataFrame)

    def test_output_columns(self):
        # Check that the output DataFrame contains the expected column names
        result = transform(make_raw_df())
        expected = {"Store_ID", "Month", "Dept", "IsHoliday", "Weekly_Sales", "CPI", "Unemployment"}
        assert set(result.columns) == expected

    def test_date_converted_to_month(self):
        # Verify that the Date column is converted to Month integers
        result = transform(make_raw_df())
        assert result["Month"].tolist() == [1, 3]  # row with 9000 filtered out

    def test_filters_low_weekly_sales(self):
        """Rows with Weekly_Sales <= 10000 must be removed."""
        result = transform(make_raw_df())
        assert (result["Weekly_Sales"] > 10000).all()

    def test_all_rows_kept_when_above_threshold(self):
        # Ensure all rows are retained when Weekly_Sales values exceed the threshold
        df = make_raw_df(Weekly_Sales=[11000.0, 12000.0, 13000.0])
        result = transform(df)
        assert len(result) == 3

    def test_no_rows_kept_when_all_below_threshold(self):
        # Verify that all rows are removed when Weekly_Sales values are below the threshold
        df = make_raw_df(Weekly_Sales=[100.0, 200.0, 300.0])
        result = transform(df)
        assert result.empty


class TestTransformMissingValues:
    def test_imputes_missing_weekly_sales(self):
        # Check that missing Weekly_Sales values are properly imputed
        df = make_raw_df(Weekly_Sales=[15000.0, None, 20000.0])
        result = transform(df)
        assert result["Weekly_Sales"].notna().all()

    def test_imputes_missing_cpi(self):
        # Verify that missing CPI values are properly imputed
        df = make_raw_df(CPI=[211.0, None, 213.0])
        result = transform(df)
        assert result["CPI"].notna().all()

    def test_imputes_missing_unemployment(self):
        # Check that missing Unemployment values are properly imputed
        df = make_raw_df(Unemployment=[8.0, None, 7.9])
        result = transform(df)
        assert result["Unemployment"].notna().all()

    def test_numeric_cols_rounded_to_two_decimals(self):
        # Ensure that numeric columns are rounded to two decimal places
        df = make_raw_df(CPI=[211.123456, 212.987654, 213.111111])
        result = transform(df)
        for val in result["CPI"]:
            assert round(val, 2) == val


class TestTransformEdgeCases:
    def test_empty_dataframe_returns_empty(self):
        # Verify that an empty input DataFrame returns an empty output DataFrame
        result = transform(pd.DataFrame())
        assert result.empty

    def test_extra_columns_are_dropped(self):
        # Check that columns not in the expected schema are removed from the output
        df = make_raw_df()
        df["extra_col"] = "noise"
        result = transform(df)
        assert "extra_col" not in result.columns

    def test_original_dataframe_not_mutated(self):
        # Ensure that the original input DataFrame is not modified by transform
        df = make_raw_df()
        original_cols = list(df.columns)
        transform(df)
        assert list(df.columns) == original_cols

    def test_invalid_date_returns_empty(self):
        # Verify that invalid date values result in an empty output DataFrame
        df = make_raw_df(Date=["not-a-date", "bad", "also-bad"])
        result = transform(df)
        assert result.empty
