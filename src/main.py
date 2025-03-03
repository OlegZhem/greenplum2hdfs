
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

class DataDestination(StrEnum):
    CSV = 'CSV'
    HDFS = 'HDFS'

def process(data_source, data_transformer, data_destination):
    if data_source == DataSource.CSV:
        if data_transformer == DataTransformer.PANDAS:
            if data_destination == DataDestination.CSV:
                from_csv_transform_pandas_to_csv()
            else:
                pass
        else:
            if data_destination == DataDestination.CSV:
                from_csv_transform_dask_to_csv()
            else:
                pass
    else:
        if data_transformer == DataTransformer.PANDAS:
            if data_destination == DataDestination.CSV:
                from_greenplum_transform_pandas_to_csv()
            else:
                pass
        else:
            if data_destination == DataDestination.CSV:
                from_greenplum_table_transform_dask_to_csv()
            else:
                pass


if __name__ == "__main__":
    process(DataSource.CSV, DataTransformer.DASK, DataDestination.CSV)