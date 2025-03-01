import psycopg2
import pandas as pd
from contextlib import contextmanager

@contextmanager
def database_connection(host, port, dbname, user, password):
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    try:
        yield conn
    finally:
        conn.close()

def data_generator(con, query, chunk_size=1000):
    with con.cursor() as cursor:  # Automatically closes the cursor
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield rows

def get_greenplum_connection_uri(**kwargs):
    """
    Generates a SQLAlchemy connection string for Greenplum.

    :param kwargs: Dictionary containing connection parameters:
                   - host
                   - port
                   - dbname
                   - user
                   - password
    :return: SQLAlchemy connection string (str)
    """
    return f"postgresql+psycopg2://{kwargs['user']}:{kwargs['password']}@{kwargs['host']}:{kwargs['port']}/{kwargs['dbname']}"



if __name__ == "__main__":

    # Usage example
    GREENPLUM_CONNECTION_PARAMS = {
        'host': 'your_host',
        'port': 'your_port',
        'dbname': 'your_db',
        'user': 'your_user',
        'password': 'your_password'
    }
    GREENPLUM_CHUNK_SIZE = 1000
    QUERY = 'SELECT * FROM your_table'

    # Use the context manager to handle the connection
    with database_connection(**GREENPLUM_CONNECTION_PARAMS) as conn:
        # Create the generator
        generator = data_generator(conn, QUERY, chunk_size=GREENPLUM_CHUNK_SIZE)

        # Collect all chunks as DataFrames
        all_data = []
        for rows in generator:
            all_data.append(pd.DataFrame(rows, columns=["column1", "column2", "column3", "column4"]))

