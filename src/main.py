
import logging
import sys
from enum import Enum, StrEnum

from src.ui_actions import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DataSource(StrEnum):
    CSV = 'CSV'
    GREENPLUM = 'GREENPLUM'
    
class DataTransformer(StrEnum):
    PANDAS = 'PANDAS'
    DASK = 'DASK'

class DataTransformerDask(StrEnum):
    TRANSFORM = 'TRANSFORM'
    AGGREGATE = 'AGGREGATE'
    FULL = 'FULL'

class DataDestination(StrEnum):
    CSV = 'CSV'
    PARQUET = 'PARQUET'
    HDFS = 'HDFS'

def process_dask(data_source, data_transformer, data_destination):
    if data_source == DataSource.CSV:
        if data_destination == DataDestination.CSV:
            from_csv_full_dask_to_csv()
        elif data_destination == DataDestination.PARQUET:
            from_csv_full_dask_to_parquet()
        else:
            pass
    else:
        if data_destination == DataDestination.CSV:
            from_greenplum_table_full_dask_to_csv()
        elif data_destination == DataDestination.CSV:
            pass
        else:
            pass

def create_histogram(data_source, column, bins=10):
    if data_source == DataSource.CSV:
        return from_csv_dask_histogram(column, bins)
    else:
        return from_greenplum_dask_histogram(column, bins)

def get_moments(data_source, column):
    if data_source == DataSource.CSV:
        return from_csv_moments(column)
    else:
        return from_greenplum_moments(column)

def get_ci_via_t_interval(data_source, column, ci_percent):
    if data_source == DataSource.CSV:
        return from_csv_ci_via_t_interval(column, ci_percent)
    else:
        return from_greenplum_ci_via_t_interval(column, ci_percent)

def get_ci_via_bootstrap(data_source, column, ci_percent):
    if data_source == DataSource.CSV:
        return from_csv_ci_via_bootstrap(column, ci_percent)
    else:
        return from_greenplum_ci_via_bootstrap(column, ci_percent)


if __name__ == "__main__":
    pass
    #process(DataSource.CSV, DataTransformer.DASK, DataDestination.CSV)