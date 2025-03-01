
import logging
import os
import sys

from dask.diagnostics import ProgressBar

from src.saver_csv import SaverCSV
import src.data_processor_pandas as dpp
import src.data_processor_dask as dpd

import pandas as pd
import dask.dataframe as dd


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(message)s')
formatter.default_msec_format = '%s.%03d'
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
fh = logging.FileHandler(__name__+".log")
fh.setFormatter(formatter)
logger.addHandler(fh)


IN_CSV_FILE_PATH = '../data/test_data_2M.csv'
PANDAS_CHUNK_SIZE = 10_000_000
OUT_PANDAS_CSV_FILE_PATH = '../out/processed_data_pandas_2M.csv'
BOCK_SIZE = "64MB"
OUT_DASK_CSV_FILE_PATH = '../out/processed_data_dask_2M.csv'
GREENPLUM_CONNECTION_PARAMS = {
    'host': 'your_host',
    'port': 'your_port',
    'dbname': 'your_db',
    'user': 'your_user',
    'password': 'your_password'
}
GREENPLUM_CHUNK_SIZE = 1000

QUERY = 'SELECT * FROM your_table'

QUERY1 = """SELECT DISTINCT
    column1,
    column2,
    CASE 
        WHEN column3 ~ '[0-9]' THEN column3 
        ELSE '' 
    END AS column3,
    column4
FROM your_table_name
WHERE 
    column3 IS NOT NULL AND column3 <> '' 
    AND (EXTRACT(HOUR FROM column4) NOT BETWEEN 1 AND 2"""

QUERY2 = """WITH Deduplicated AS (
    SELECT DISTINCT ON (column1, column2, column3, column4) *
    FROM your_table
    ORDER BY column1, column2, column3, column4
)
SELECT 
    column1, 
    column2, 
    CASE 
        WHEN column3 ~ '[0-9]' THEN column3 
        ELSE '' 
    END AS column3, 
    column4
FROM Deduplicated
WHERE column3 IS NOT NULL AND column3 <> '' 
AND EXTRACT(HOUR FROM column4) NOT BETWEEN 1 AND 2"""


def from_csv_transform_pandas_to_csv():
    try:
        with SaverCSV(OUT_PANDAS_CSV_FILE_PATH) as saver:
            logger.info(f'start processing {IN_CSV_FILE_PATH} by {PANDAS_CHUNK_SIZE} rows')
            for chunk in pd.read_csv(IN_CSV_FILE_PATH, chunksize=PANDAS_CHUNK_SIZE):
                chunk_processed = dpp.transform_data_frame(chunk)
                saver.async_save(chunk_processed)
        logger.info(f'finish processing {IN_CSV_FILE_PATH} to {OUT_PANDAS_CSV_FILE_PATH}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=True)

def from_csv_transform_dask_to_csv():
    try:
        with SaverCSV(OUT_DASK_CSV_FILE_PATH) as saver:
            logger.info(f'start processing {IN_CSV_FILE_PATH} by {BOCK_SIZE}')
            df_dask = dd.read_csv(IN_CSV_FILE_PATH, blocksize=BOCK_SIZE)
            with ProgressBar():
                df_processed = dpd.transform_data_frame_all(df_dask).compute()
            saver.save(df_processed)
        logger.info(f'finish processing {IN_CSV_FILE_PATH} to {OUT_DASK_CSV_FILE_PATH}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=True)


if __name__ == "__main__":
    #from_csv_transform_pandas_to_csv()
    from_csv_transform_dask_to_csv()