import logging
import sys

import pandas as pd
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
from distributed import Client, progress

import src.data_processor_pandas as dpp
import src.data_processor_dask as dpd
import src.greenplum_connector as gpc
import src.queries as queries
from src.ui_properties import settings
from src.saver_csv import SaverCSV
from src import _stdout_handler, _file_handler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(_stdout_handler)
logger.addHandler(_file_handler)


def from_csv_transform_pandas_to_csv():
    try:
        with SaverCSV(settings["OUT_PANDAS_CSV_FILE_PATH"]) as saver:
            logger.info(f'start processing {settings["IN_CSV_FILE_PATH"]} by {settings["PANDAS_CHUNK_SIZE"]} rows')
            for chunk in pd.read_csv(settings["IN_CSV_FILE_PATH"], chunksize=settings["PANDAS_CHUNK_SIZE"]):
                chunk_processed = dpp.transform_data_frame(chunk)
                saver.async_save(chunk_processed)
        logger.info(f'finish processing {settings["IN_CSV_FILE_PATH"]} to {settings["OUT_PANDAS_CSV_FILE_PATH"]}')
    except Exception as e:
        logger.error("Exception:", exc_info=e)
        raise

def from_csv_full_dask_to_csv():
    try:
        with Client() as client:
            logger.info(f'start processing {settings["IN_CSV_FILE_PATH"]} by {settings["BOCK_SIZE"]}')
            df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"], blocksize=settings["BOCK_SIZE"])
            df_tran = dpd.transform_data_frame(df_dask)
            df_agg = dpd.aggregate_data_frame(df_tran)
            df_processed = dpd.merge_with_aggregated(df_tran, df_agg)
            progress(client.persist(df_processed))
            df_processed.to_csv(settings["OUT_DASK_CSV_FILE_PATH"], index=False, compute=True, single_file=True)
            logger.info(f'finish processing {settings["IN_CSV_FILE_PATH"]} to {settings["OUT_DASK_CSV_FILE_PATH"]}')
    except Exception as e:
        logger.error("Exception:", exc_info=e)
        raise

def from_greenplum_transform_pandas_to_csv():
    try:
        with SaverCSV(settings["OUT_PANDAS_CSV_FILE_PATH"]) as saver:
            logger.info(f'start processing greenplum {queries.QUERY_TABLE} by {settings["GREENPLUM_CHUNK_SIZE"]} rows')
            with gpc.database_connection(**settings["GREENPLUM_CONNECTION_PARAMS"]) as conn:
                generator = gpc.data_generator(conn, queries.QUERY_TABLE, chunk_size=settings["GREENPLUM_CHUNK_SIZE"])
                for rows in generator:
                    chunk = pd.DataFrame(rows, columns=["column1", "column2", "column3", "column4"])
                    chunk_processed = dpp.transform_data_frame(chunk)
                    saver.async_save(chunk_processed)
        logger.info(f'finish processing greenplum {queries.QUERY_TABLE} to {settings["OUT_PANDAS_CSV_FILE_PATH"]}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=e)
        raise

def from_greenplum_table_transform_dask_to_csv():
    try:
        with Client() as client:
                logger.info(f'start processing greenplum table {settings["GREENPLUM_TABLE_NAME"]} in {settings["DASK_PARTITIONS"]} partitions')
                df_dask = dpd.load_table_with_dask_sqlalchemy(
                    settings["GREENPLUM_TABLE_NAME"],
                    "column4",
                    settings["DASK_PARTITIONS"],
                    **settings["GREENPLUM_CONNECTION_PARAMS"])
                df_tran = dpd.transform_data_frame(df_dask)
                df_agg = dpd.aggregate_data_frame(df_tran)
                df_processed = dpd.merge_with_aggregated(df_tran, df_agg)
                progress(client.persist(df_processed))
                df_processed.to_csv(settings["OUT_DASK_CSV_FILE_PATH"], index=False, compute=True, single_file=True)
    logger.info(f'finish processing greenplum table {settings["GREENPLUM_TABLE_NAME"]} to {settings["OUT_DASK_CSV_FILE_PATH"]}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=e)
        raise

def from_greenplum_query_load_dask_to_csv():
    try:
        with Client() as client:
            logger.info(f'start processing greenplum query {queries.QUERY_TRANSFORM_1} in {settings["DASK_PARTITIONS"]} partitions')
            df_processed = dpd.load_query_with_dask_sqlalchemy(
                queries.create_selectable2(settings["GREENPLUM_TABLE_NAME"]),
                settings["DASK_PARTITIONS"],
                **settings["GREENPLUM_CONNECTION_PARAMS"])
            progress(client.persist(df_processed))
            df_processed.to_csv(settings["OUT_DASK_CSV_FILE_PATH"], index=False, compute=True, single_file=True)
            logger.info(f'finish processing greenplum query {queries.QUERY_TRANSFORM_1} to {settings["OUT_DASK_CSV_FILE_PATH"]}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=e)
        raise

if __name__ == "__main__":
    from_csv_full_dask_to_csv()