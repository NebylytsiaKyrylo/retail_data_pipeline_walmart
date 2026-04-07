import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "db_user": os.environ.get("POSTGRES_USER"),
    "db_password": os.environ.get("POSTGRES_PASSWORD"),
    "db_host": os.environ.get("POSTGRES_HOST"),
    "db_port": os.environ.get("POSTGRES_PORT"),
    "db_name": os.environ.get("POSTGRES_DB"),
}

# Get the project root (config.py is at the root)
PROJECT_ROOT = Path(__file__).parent

DATA_PATHS = {
    "parquet_input": str(PROJECT_ROOT / "data" / "raw_data" / "extra_data.parquet"),
    "clean_output": str(PROJECT_ROOT / "data" / "output" / "clean_data.csv"),
    "agg_output": str(PROJECT_ROOT / "data" / "output" / "agg_data.csv"),
    "data_output": str(PROJECT_ROOT / "data" / "output"),
}
