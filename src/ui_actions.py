import logging
import sys

import pandas as pd
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
from distributed import Client

import src.data_processor_pandas as dpp
import src.data_processor_dask as dpd
import src.greenplum_connector as gpc
import src.queries as queries
from src.ui_properties import *
from src.saver_csv import SaverCSV

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(message)s')
formatter.default_msec_format = '%s.%03d'
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
fh = logging.FileHandler("../logs/log.log")
fh.setFormatter(formatter)
logger.addHandler(fh)

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
        with Client() as client:
            with SaverCSV(OUT_DASK_CSV_FILE_PATH) as saver:
                logger.info(f'start processing {IN_CSV_FILE_PATH} by {BOCK_SIZE}')
                df_dask = dd.read_csv(IN_CSV_FILE_PATH, blocksize=BOCK_SIZE)
                with ProgressBar():
                    df_processed = dpd.transform_data_frame(df_dask).compute()
                saver.save(df_processed)
            logger.info(f'finish processing {IN_CSV_FILE_PATH} to {OUT_DASK_CSV_FILE_PATH}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=True)


def from_greenplum_transform_pandas_to_csv():
    try:
        with SaverCSV(OUT_PANDAS_CSV_FILE_PATH) as saver:
            logger.info(f'start processing greenplum {queries.QUERY} by {GREENPLUM_CHUNK_SIZE} rows')
            with gpc.database_connection(**GREENPLUM_CONNECTION_PARAMS) as conn:
                generator = gpc.data_generator(conn, queries.QUERY, chunk_size=GREENPLUM_CHUNK_SIZE)
                for rows in generator:
                    chunk = pd.DataFrame(rows, columns=["column1", "column2", "column3", "column4"])
                    chunk_processed = dpp.transform_data_frame(chunk)
                    saver.async_save(chunk_processed)
        logger.info(f'finish processing greenplum {queries.QUERY} to {OUT_PANDAS_CSV_FILE_PATH}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=True)

def from_greenplum_table_transform_dask_to_csv():
    try:
        with Client() as client:
            with SaverCSV(OUT_DASK_CSV_FILE_PATH) as saver:
                logger.info(f'start processing greenplum table {GREENPLUM_TABLE_NAME} in {DASK_PARTITIONS} partition')
                with ProgressBar():
                    df_dask = dpd.load_table_with_dask_sqlalchemy(
                        GREENPLUM_TABLE_NAME, "column4", DASK_PARTITIONS, **GREENPLUM_CONNECTION_PARAMS)
                    df_processed = dpd.transform_data_frame(df_dask).compute()
                saver.save(df_processed)
            logger.info(f'finish processing greenplum table {GREENPLUM_TABLE_NAME} to {OUT_DASK_CSV_FILE_PATH}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=True)

def from_greenplum_query_load_dask_to_csv():
    try:
        with Client() as client:
            with SaverCSV(OUT_DASK_CSV_FILE_PATH) as saver:
                logger.info(f'start processing greenplum query {queries.QUERY1} in {DASK_PARTITIONS} partition')
                with ProgressBar():
                    df_dask = dpd.load_query_with_dask_sqlalchemy(
                        queries.create_selectable2(GREENPLUM_TABLE_NAME), DASK_PARTITIONS, **GREENPLUM_CONNECTION_PARAMS)
                saver.save(df_dask)
            logger.info(f'finish processing greenplum query {queries.QUERY1} to {OUT_DASK_CSV_FILE_PATH}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=True)
