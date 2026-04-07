import pytest
from unittest.mock import patch

from pipeline.validation import validation

FAKE_PATHS = {
    "clean_output": "/fake/output/clean_data.csv",
    "agg_output": "/fake/output/agg_data.csv",
}


@pytest.fixture(autouse=True)
def patch_data_paths():
    with patch("pipeline.validation.DATA_PATHS", FAKE_PATHS):
        yield


class TestValidation:
    # Test that validation returns True when both output files exist in the file system
    def test_returns_true_when_both_files_exist(self):
        with patch("os.path.exists", return_value=True):
            assert validation() is True

    # Test that validation returns False when the clean data output file is missing
    def test_returns_false_when_clean_file_missing(self):
        def fake_exists(path):
            return path != FAKE_PATHS["clean_output"]

        with patch("os.path.exists", side_effect=fake_exists):
            assert validation() is False

    # Test that validation returns False when the aggregated data output file is missing
    def test_returns_false_when_agg_file_missing(self):
        def fake_exists(path):
            return path != FAKE_PATHS["agg_output"]

        with patch("os.path.exists", side_effect=fake_exists):
            assert validation() is False

    # Test that validation returns False when both output files are missing from the file system
    def test_returns_false_when_both_files_missing(self):
        with patch("os.path.exists", return_value=False):
            assert validation() is False
