
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
GREENPLUM_TABLE_NAME= 'table1'
DASK_PARTITIONS = 10
GREENPLUM_CHUNK_SIZE = 1000