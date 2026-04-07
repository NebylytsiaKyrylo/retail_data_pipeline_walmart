import pandas as pd
import pytest

from pipeline.aggregate import aggregate


def make_clean_df():
    return pd.DataFrame({
        "Store_ID": [1, 1, 2, 2],
        "Month": [1, 1, 2, 3],
        "Dept": [1, 2, 1, 1],
        "IsHoliday": [False, True, False, False],
        "Weekly_Sales": [20000.0, 30000.0, 15000.0, 25000.0],
        "CPI": [211.0, 212.0, 213.0, 214.0],
        "Unemployment": [8.0, 8.1, 7.9, 7.8],
    })


class TestAggregateHappyPath:
    # Test that the aggregate function returns a pandas DataFrame object
    def test_returns_dataframe(self):
        result = aggregate(make_clean_df())
        assert isinstance(result, pd.DataFrame)

    # Test that the output DataFrame contains exactly the expected columns: Month and Weekly_Sales
    def test_output_columns(self):
        result = aggregate(make_clean_df())
        assert set(result.columns) == {"Month", "Weekly_Sales"}

    # Test that the output has exactly one row per unique month value
    def test_one_row_per_month(self):
        result = aggregate(make_clean_df())
        assert len(result) == 3  # months 1, 2, 3

    # Test that the mean calculation is correct for a specific month
    def test_mean_calculation(self):
        result = aggregate(make_clean_df())
        month1 = result[result["Month"] == 1]["Weekly_Sales"].iloc[0]
        assert month1 == 25000.0  # (20000 + 30000) / 2

    # Test that the Weekly_Sales values are rounded to two decimal places
    def test_rounded_to_two_decimals(self):
        df = pd.DataFrame({"Month": [1, 1], "Weekly_Sales": [10000.1, 20000.2]})
        result = aggregate(df)
        val = result["Weekly_Sales"].iloc[0]
        assert round(val, 2) == val

    # Test aggregation when all rows belong to the same single month
    def test_single_month(self):
        df = pd.DataFrame({"Month": [5, 5], "Weekly_Sales": [12000.0, 18000.0]})
        result = aggregate(df)
        assert len(result) == 1
        assert result["Weekly_Sales"].iloc[0] == 15000.0


class TestAggregateEdgeCases:
    # Test that an empty input DataFrame returns an empty output DataFrame
    def test_empty_dataframe_returns_empty(self):
        result = aggregate(pd.DataFrame())
        assert result.empty

    # Test aggregation when the input DataFrame contains only a single row
    def test_single_row(self):
        df = pd.DataFrame({"Month": [4], "Weekly_Sales": [22000.0]})
        result = aggregate(df)
        assert len(result) == 1
        assert result["Weekly_Sales"].iloc[0] == 22000.0

    # Test that the output DataFrame is sorted by Month in ascending order
    def test_months_sorted(self):
        df = pd.DataFrame({"Month": [3, 1, 2], "Weekly_Sales": [10000.0, 20000.0, 30000.0]})
        result = aggregate(df)
        assert list(result["Month"]) == sorted(result["Month"].tolist())
