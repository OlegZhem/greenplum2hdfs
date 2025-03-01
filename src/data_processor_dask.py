import dask.dataframe as dd
from dask.distributed import Client
from dask.diagnostics import ProgressBar

from sqlalchemy import create_engine, text
from sqlalchemy.sql import select
from sqlalchemy.schema import Table, MetaData

from src.greenplum_connector import get_greenplum_connection_uri


# Function to check if a string contains digits
def contains_digits(s):
    return any(char.isdigit() for char in s)

def transform_data_frame(df):
    # Remove rows where column3 is empty
    df = df[df["column3"].notnull() & (df["column3"].str.strip() != "")]

    # Remove rows where column4 (timestamp) is between 1 AM and 3 AM
    df["column4"] = dd.to_datetime(df["column4"], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce')  # Ensure column4 is datetime
    df = df[~((df["column4"].dt.hour >= 1) & (df["column4"].dt.hour < 3))]

    # If column3 does not contain digits, set it to an empty string
    df["column3"] = df["column3"].apply(lambda x: x if contains_digits(x) else "", meta=('column3', 'object'))

    # Remove duplicates while keeping one instance
    df = df.drop_duplicates()

    return df

def load_table_with_dask_sqlalchemy(table_name, index_col, npartitions=10, **kwargs):
    """
    Loads data from Greenplum into a Dask DataFrame using SQLAlchemy.

    :param table_name: Name of the table to load.
    :param index_col: Column to use as the index (must be unique & indexed in DB).
    :param npartitions: Number of partitions for parallel loading.
    :param kwargs: Greenplum connection parameters (host, port, dbname, user, password).
    :return: Dask DataFrame
    """
    connection_uri = get_greenplum_connection_uri(**kwargs)
    ddf = dd.read_sql_table(table_name, con=connection_uri, index_col=index_col, npartitions=npartitions)
    return ddf

def load_query_with_dask_sqlalchemy(sql, npartitions=10, **kwargs):
    """
     Loads data from Greenplum into a Dask DataFrame using SQLAlchemy.

     :param sql : SQLAlchemy Selectable. SQL query to be executed. TextClause is not supported
     :param npartitions: Number of partitions for parallel loading.
     :param kwargs: Greenplum connection parameters (host, port, dbname, user, password).
     :return: Dask DataFrame
     """
    connection_uri = get_greenplum_connection_uri(**kwargs)
    ddf = dd.read_sql_query( sql, con=connection_uri, npartitions=npartitions)
    return ddf

if __name__ == "__main__":

    # Usage example
    GREENPLUM_CONNECTION_PARAMS = {
        'host': 'your_host',
        'port': 'your_port',
        'dbname': 'your_db',
        'user': 'your_user',
        'password': 'your_password'
    }
    BOCK_SIZE = "64MB"
    with Client() as client:
        df_dask = load_table_with_dask_sqlalchemy("my_table", "column4", **GREENPLUM_CONNECTION_PARAMS)
        with ProgressBar():
            df_processed = transform_data_frame(df_dask).compute()
