import os
import pandas as pd
import pytest
from unittest.mock import patch

from pipeline.load import load


def make_df():
    return pd.DataFrame({"Month": [1, 2], "Weekly_Sales": [15000.0, 20000.0]})


FAKE_PATHS = {
    "data_output": "/tmp/test_pipeline_output",
    "clean_output": "/tmp/test_pipeline_output/clean_data.csv",
    "agg_output": "/tmp/test_pipeline_output/agg_data.csv",
}


@pytest.fixture(autouse=True)
def patch_data_paths():
    with patch("pipeline.load.DATA_PATHS", FAKE_PATHS):
        yield
    # Cleanup
    for f in [FAKE_PATHS["clean_output"], FAKE_PATHS["agg_output"]]:
        if os.path.exists(f):
            os.remove(f)
    if os.path.exists(FAKE_PATHS["data_output"]):
        os.rmdir(FAKE_PATHS["data_output"])


class TestLoadHappyPath:
    def test_creates_output_directory(self):
        # Verify that the output directory is created when load() is called
        load(make_df(), make_df())
        assert os.path.isdir(FAKE_PATHS["data_output"])

    def test_clean_csv_is_created(self):
        # Verify that the clean data CSV file is created
        load(make_df(), make_df())
        assert os.path.exists(FAKE_PATHS["clean_output"])

    def test_agg_csv_is_created(self):
        # Verify that the aggregated data CSV file is created
        load(make_df(), make_df())
        assert os.path.exists(FAKE_PATHS["agg_output"])

    def test_clean_csv_content(self):
        # Verify that the clean CSV file content matches the input DataFrame
        clean = make_df()
        load(clean, make_df())
        result = pd.read_csv(FAKE_PATHS["clean_output"])
        pd.testing.assert_frame_equal(result, clean)

    def test_agg_csv_content(self):
        # Verify that the aggregated CSV file content matches the input DataFrame
        agg = make_df()
        load(make_df(), agg)
        result = pd.read_csv(FAKE_PATHS["agg_output"])
        pd.testing.assert_frame_equal(result, agg)

    def test_no_index_column_written(self):
        # Verify that no index column is written to the CSV file
        load(make_df(), make_df())
        result = pd.read_csv(FAKE_PATHS["clean_output"])
        assert "Unnamed: 0" not in result.columns


class TestLoadEdgeCases:
    def test_both_empty_still_creates_files(self):
        # Verify that CSV files are still created even when both input DataFrames are empty
        load(pd.DataFrame(), pd.DataFrame())
        assert os.path.exists(FAKE_PATHS["clean_output"])
        assert os.path.exists(FAKE_PATHS["agg_output"])
