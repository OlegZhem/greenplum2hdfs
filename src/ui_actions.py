import datetime
import logging
import os
import sys
from contextlib import contextmanager

import pandas as pd
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
from distributed import Client, progress, LocalCluster

import src.data_processor_pandas as dpp
import src.data_processor_dask as dpd
import src.greenplum_connector as gpc
import src.queries as queries
from src.confidence_interval import *
from src.ui_properties import settings
from src.saver_csv import SaverCSV
from src import _stdout_handler, _file_handler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#logger.addHandler(_stdout_handler)
#logger.addHandler(_file_handler)


@contextmanager
def dask_default_cluster():
    try:
        with LocalCluster(n_workers=6, threads_per_worker=2, memory_limit='8GB') as cluster:
            with Client(cluster) as client:
                print(cluster)
                yield client
    except Exception as e:
        logger.error("Exception:", exc_info=e)
        raise


def from_csv_full_dask_to_csv():
    with dask_default_cluster() as client:
        logger.info(f'start processing {settings["IN_CSV_FILE_PATH"]} by {settings["DASK_BLOCK_SIZE"]}')
        df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"],
                              #parse_dates=['column4'],
                              #date_parser= lambda  x : datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S:%f'),
                              blocksize=settings["DASK_BLOCK_SIZE"])
        df_dask["column4"] = dd.to_datetime(df_dask["column4"], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce')  # Ensure column4 is datetime
        df_processed = full_dask(df_dask)
        dask_to_csv(df_processed)

def from_csv_full_dask_to_parquet():
    with dask_default_cluster() as client:
        logger.info(f'start processing {settings["IN_CSV_FILE_PATH"]} by {settings["DASK_BLOCK_SIZE"]}')
        df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"],
                              #parse_dates=['column4'],
                              #date_parser= lambda  x : datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S:%f'),
                              blocksize=settings["DASK_BLOCK_SIZE"])
        df_dask["column4"] = dd.to_datetime(df_dask["column4"], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce')  # Ensure column4 is datetime
        df_processed = full_dask(df_dask)
        dask_to_parquet(df_processed)

def from_csv_full_dask_to_hdfs():
    with dask_default_cluster() as client:
        logger.info(f'start processing {settings["IN_CSV_FILE_PATH"]} by {settings["DASK_BLOCK_SIZE"]}')
        df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"],
                              #parse_dates=['column4'],
                              #date_parser= lambda  x : datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S:%f'),
                              blocksize=settings["DASK_BLOCK_SIZE"])
        df_dask["column4"] = dd.to_datetime(df_dask["column4"], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce')  # Ensure column4 is datetime
        df_processed = full_dask(df_dask)
        dask_to_hdfs(df_processed)

def from_greenplum_table_full_dask_to_csv():
    try:
        with LocalCluster(n_workers=12, threads_per_worker=1, memory_limit='10GB') as cluster:
            with Client(cluster) as client:
                print(cluster)
                logger.info(f'start processing greenplum table {settings["GREENPLUM_TABLE_NAME"]} in {settings["DASK_PARTITIONS"]} partitions')
                df_dask = dpd.greenplum_load_table_dask(
                    settings["GREENPLUM_TABLE_NAME"],
                    "column4",
                    settings["DASK_PARTITIONS"],
                    **settings["GREENPLUM_CONNECTION_PARAMS"])
                df_processed = full_dask(df_dask)
                dask_to_csv(df_processed)
    except Exception as e:
        logger.error("An exception occurred:", exc_info=e)
        raise

def from_greenplum_table_full_dask_to_parquet():
    with dask_default_cluster() as client:
        logger.info(f'start processing greenplum table {settings["GREENPLUM_TABLE_NAME"]} in {settings["DASK_PARTITIONS"]} partitions')
        df_dask = dpd.greenplum_load_table_dask(
            settings["GREENPLUM_TABLE_NAME"],
            "column4",
            settings["DASK_PARTITIONS"],
            **settings["GREENPLUM_CONNECTION_PARAMS"])
        df_processed = full_dask(df_dask)
        dask_to_parquet(df_processed)

def from_greenplum_table_full_dask_to_hdfs():
    with dask_default_cluster() as client:
        logger.info(f'start processing greenplum table {settings["GREENPLUM_TABLE_NAME"]} in {settings["DASK_PARTITIONS"]} partitions')
        df_dask = dpd.greenplum_load_table_dask(
            settings["GREENPLUM_TABLE_NAME"],
            "column4",
            settings["DASK_PARTITIONS"],
            **settings["GREENPLUM_CONNECTION_PARAMS"])
        df_processed = full_dask(df_dask)
        dask_to_hdfs(df_processed)

def full_dask(df_dask, client=None):
    # Analyze partition sizes before transformation
    #partition_sizes = df_dask.map_partitions(len).compute()
    # Print summary of partition sizes
    #print("Partition Size Summary Before Transformation:")
    #print(partition_sizes.describe())  # Gives min, max, mean, etc.
    #print(partition_sizes)  # Print all partition sizes

    #print(df_dask.divisions)

    df_tran = dpd.transform_data_frame(df_dask)
    df_agg = dpd.aggregate_data_frame(df_tran).persist()
    df_processed = dpd.merge_with_aggregated_3(df_tran, df_agg)
    if client is not None:
        progress(client.persist(df_processed))

    # Print execution plan to check for shuffles
    #optimized_plan = df_processed.__dask_optimize__(df_processed.dask, df_processed.__dask_keys__())
    #print(optimized_plan)
    # Visualize task graph
    #df_processed.visualize(filename="dask_execution_plan.svg")

    return df_processed

def dask_to_csv(df_processed):
    df_processed.to_csv(settings["OUT_DASK_CSV_FILE_PATH"], index=False, compute=True, single_file=True)
    logger.info(f'Finished processing to: {settings["OUT_DASK_CSV_FILE_PATH"]}')

def dask_to_parquet(df_processed):
    df_processed.to_parquet(settings["OUT_DASK_PARQUET_FILE_PATH"], index=False, compute=True)
    logger.info(f'Finished processing to: {settings["OUT_DASK_PARQUET_FILE_PATH"]}')

def dask_to_hdfs(df_processed):
    if settings["HDFS_FILE_FORMAT"] == 'parquet':
        df_processed.to_parquet(settings["HDFS_URI"], index=False, compute=True, engine='pyarrow')
        logger.info(f'Finished processing to HDFS (Parquet): {settings["HDFS_URI"]}')
    elif settings["HDFS_FILE_FORMAT"] == 'csv':
        df_processed.to_csv(settings["HDFS_URI"], index=False, compute=True, single_file=True, storage_options={'protocol': 'hdfs'})
        logger.info(f'Finished processing to HDFS (CSV): {settings["HDFS_URI"]}')
    else:
        raise ValueError("Unsupported file format. Choose 'parquet' or 'csv'.")


def from_greenplum_query_load_dask_to_csv():
    try:
        with Client() as client:
            logger.info(f'start processing greenplum query {queries.QUERY_TRANSFORM_1} in {settings["DASK_PARTITIONS"]} partitions')
            df_processed = dpd.greenplum_load_query_dask_sqlalchemy(
                queries.create_transformation_selectable2(settings["GREENPLUM_TABLE_NAME"]),
                settings["DASK_PARTITIONS"],
                **settings["GREENPLUM_CONNECTION_PARAMS"])
            progress(client.persist(df_processed))
            df_processed.to_csv(settings["OUT_DASK_CSV_FILE_PATH"], index=False, compute=True, single_file=True)
            logger.info(f'finish processing greenplum query {queries.QUERY_TRANSFORM_1} to {settings["OUT_DASK_CSV_FILE_PATH"]}')
    except Exception as e:
        logger.error("An exception occurred:", exc_info=e)
        raise

def from_csv_dask_histogram(column, bins=10):
    with dask_default_cluster() as client:
        logger.info(f'create histogram for {column} in {settings["IN_CSV_FILE_PATH"]} file')
        df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"], blocksize=settings["DASK_BLOCK_SIZE"], usecols=[column])
        return dpd.get_histogram(df_dask, column, bins)

def from_greenplum_dask_histogram(column, bins=10):
    try:
        with Client() as client:
            logger.info(f'create histogram for {column} in greenplum table {settings["GREENPLUM_TABLE_NAME"]}')
            df_dask = dpd.greenplum_load_query_dask_sqlalchemy(
                queries.create_selectable_for_column(column, settings["GREENPLUM_TABLE_NAME"]),
                settings["DASK_PARTITIONS"],
                **settings["GREENPLUM_CONNECTION_PARAMS"])
            return dpd.get_histogram(df_dask, column, bins)
    except Exception as e:
        logger.error("An exception occurred:", exc_info=e)
        raise

def from_csv_moments(column):
    with dask_default_cluster() as client:
            logger.info(f'calculate moments for {column} in {settings["IN_CSV_FILE_PATH"]} file')
            df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"], blocksize=settings["DASK_BLOCK_SIZE"], usecols=[column])
            return dpd.get_statistic(df_dask, column)

def from_csv_ci_via_t_interval(column, ci):
    with dask_default_cluster() as client:
            logger.info(f'calculate confidential interval {ci} for {column} in {settings["IN_CSV_FILE_PATH"]} file')
            df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"], blocksize=settings["DASK_BLOCK_SIZE"], usecols=[column])
            da_dask = df_dask[column].to_dask_array(lengths=True)
            return ci_via_t_interval_dask(da_dask, confidence_level=ci)

def from_csv_ci_via_bootstrap(column, ci):
    with dask_default_cluster() as client:
            logger.info(f'calculate confidential interval {ci} for {column} in {settings["IN_CSV_FILE_PATH"]} file')
            df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"], blocksize=settings["DASK_BLOCK_SIZE"], usecols=[column])
            da_dask = df_dask[column].to_dask_array(lengths=True)
            return ci_via_bootstrap(da_dask, confidence_level=ci)

def from_greenplum_moments(column):
    with dask_default_cluster() as client:
        logger.info(f'calculate moments for {column} in greenplum table {settings["GREENPLUM_TABLE_NAME"]}')
        df_dask = dpd.greenplum_load_query_dask_sqlalchemy(
            queries.create_selectable_for_column(column, settings["GREENPLUM_TABLE_NAME"]),
            settings["DASK_PARTITIONS"],
            **settings["GREENPLUM_CONNECTION_PARAMS"])
        return dpd.get_statistic(df_dask, column)

def from_greenplum_ci_via_t_interval(column, ci):
    with dask_default_cluster() as client:
        logger.info(f'calculate confidential interval {ci} for {column} in greenplum table {settings["GREENPLUM_TABLE_NAME"]}')
        df_dask = dpd.greenplum_load_query_dask_sqlalchemy(
            queries.create_selectable_for_column(column, settings["GREENPLUM_TABLE_NAME"]),
            settings["DASK_PARTITIONS"],
            **settings["GREENPLUM_CONNECTION_PARAMS"])
        da_dask = df_dask[column].to_dask_array(lengths=True)
        return ci_via_t_interval_dask(da_dask, confidence_level=ci)

def from_greenplum_ci_via_bootstrap(column, ci):
    with dask_default_cluster() as client:
        logger.info(f'calculate confidential interval {ci} for {column} in greenplum table {settings["GREENPLUM_TABLE_NAME"]}')
        df_dask = dpd.greenplum_load_query_dask_sqlalchemy(
            queries.create_selectable_for_column(column, settings["GREENPLUM_TABLE_NAME"]),
            settings["DASK_PARTITIONS"],
            **settings["GREENPLUM_CONNECTION_PARAMS"])
        da_dask = df_dask[column].to_dask_array(lengths=True)
        return ci_via_bootstrap(da_dask, confidence_level=ci)


if __name__ == "__main__":
    from_csv_full_dask_to_csv()