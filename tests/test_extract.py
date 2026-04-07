import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from pipeline.extract import extract


def make_sales_df():
    return pd.DataFrame({
        "index": [1, 2, 3],
        "Store_ID": [1, 1, 2],
        "Date": ["2022-01-07", "2022-02-04", "2022-03-04"],
        "Dept": [1, 2, 1],
        "IsHoliday": [False, True, False],
        "Weekly_Sales": [15000.0, 9000.0, 20000.0],
    })


def make_extra_df():
    return pd.DataFrame({
        "index": [1, 2, 3],
        "CPI": [211.1, 212.5, 213.0],
        "Unemployment": [8.1, 8.0, 7.9],
    })


@pytest.fixture
def mock_engine():
    return MagicMock()


class TestExtractHappyPath:
    # Test that extract function returns a non-empty merged DataFrame
    def test_returns_merged_dataframe(self, mock_engine, tmp_path):
        parquet_path = str(tmp_path / "extra_data.parquet")
        make_extra_df().to_parquet(parquet_path, index=False)

        with patch("pipeline.extract.pd.read_sql", return_value=make_sales_df()):
            result = extract(mock_engine, parquet_path)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    # Test that the merge includes columns from both sales and extra data
    def test_merged_on_index_column(self, mock_engine, tmp_path):
        parquet_path = str(tmp_path / "extra_data.parquet")
        make_extra_df().to_parquet(parquet_path, index=False)

        with patch("pipeline.extract.pd.read_sql", return_value=make_sales_df()):
            result = extract(mock_engine, parquet_path)

        assert "CPI" in result.columns
        assert "Weekly_Sales" in result.columns

    # Test that inner join returns the correct number of matching rows
    def test_row_count_after_inner_join(self, mock_engine, tmp_path):
        parquet_path = str(tmp_path / "extra_data.parquet")
        make_extra_df().to_parquet(parquet_path, index=False)

        with patch("pipeline.extract.pd.read_sql", return_value=make_sales_df()):
            result = extract(mock_engine, parquet_path)

        assert len(result) == 3


class TestExtractEdgeCases:
    # Test that extract returns empty DataFrame when parquet file does not exist
    def test_returns_empty_when_parquet_not_found(self, mock_engine):
        result = extract(mock_engine, "/nonexistent/path/extra_data.parquet")
        assert result.empty

    # Test that extract returns empty DataFrame when SQL query returns no data
    def test_returns_empty_when_sql_returns_empty(self, mock_engine, tmp_path):
        parquet_path = str(tmp_path / "extra_data.parquet")
        make_extra_df().to_parquet(parquet_path, index=False)

        with patch("pipeline.extract.pd.read_sql", return_value=pd.DataFrame()):
            result = extract(mock_engine, parquet_path)

        assert result.empty

    # Test that extract returns empty DataFrame when parquet file contains no rows
    def test_returns_empty_when_parquet_is_empty(self, mock_engine, tmp_path):
        parquet_path = str(tmp_path / "extra_data.parquet")
        pd.DataFrame(columns=["index", "CPI", "Unemployment"]).to_parquet(parquet_path, index=False)

        with patch("pipeline.extract.pd.read_sql", return_value=make_sales_df()):
            result = extract(mock_engine, parquet_path)

        assert result.empty

    # Test that extract handles SQL exceptions gracefully and returns empty DataFrame
    def test_returns_empty_on_sql_exception(self, mock_engine, tmp_path):
        parquet_path = str(tmp_path / "extra_data.parquet")
        make_extra_df().to_parquet(parquet_path, index=False)

        with patch("pipeline.extract.pd.read_sql", side_effect=Exception("DB error")):
            result = extract(mock_engine, parquet_path)

        assert result.empty

    def test_partial_index_overlap(self, mock_engine, tmp_path):
        """Only matching index values should appear in the result (inner join)."""
        parquet_path = str(tmp_path / "extra_data.parquet")
        extra = pd.DataFrame({"index": [1, 2], "CPI": [211.0, 212.0], "Unemployment": [8.0, 7.9]})
        extra.to_parquet(parquet_path, index=False)

        sales = make_sales_df()  # has index 1, 2, 3
        with patch("pipeline.extract.pd.read_sql", return_value=sales):
            result = extract(mock_engine, parquet_path)

        assert len(result) == 2
        assert set(result["index"]) == {1, 2}
