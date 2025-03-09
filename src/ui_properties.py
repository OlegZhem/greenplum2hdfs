import os
import json
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


SETTINGS_FILE = "settings.json"

DEFAULT_SETTINGS = {
    "IN_CSV_FILE_PATH": "../data/test_data_2M.csv",
    "OUT_PANDAS_CSV_FILE_PATH": "out/processed_data_pandas_2M.csv",
    "DASK_BLOCK_SIZE": "64MB",
    "OUT_DASK_CSV_FILE_PATH": "out/processed_data_dask_2M.csv",
    "OUT_DASK_PARQUET_FILE_PATH": "out/processed_data_dask_2M.parquet",
    "GREENPLUM_CONNECTION_PARAMS": {
        "host": "your_host",
        "port": "your_port",
        "dbname": "your_db",
        "user": "your_user",
        "password": "your_password"
    },
    "GREENPLUM_TABLE_NAME": "test_table",
    "DASK_PARTITIONS": 10,
    "GREENPLUM_CHUNK_SIZE": 1000
}

# Load settings from JSON file or use defaults
def load_settings():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r") as file:
                return json.load(file)
    except Exception as e:
        logger.error("Exception:", exc_info=e)
    return DEFAULT_SETTINGS.copy()

# Save settings to JSON file
def save_settings(settings):
    with open(SETTINGS_FILE, "w") as file:
        json.dump(settings, file, indent=4)

# Initialize settings when module is imported
settings = load_settings()

