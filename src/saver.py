# saver.py
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os
from pyarrow import parquet as pq
from pyarrow import Table
from hdfs import InsecureClient


class Saver:
    def __init__(self, save_to_hdfs=True, hdfs_url=None, hdfs_path=None, csv_file_path=None):
        self.save_to_hdfs = save_to_hdfs
        self.hdfs_path = hdfs_path
        self.hdfs_url = hdfs_url
        self.csv_file_path = csv_file_path
        self.executor = None
        self.first_chunk = True

    def __enter__(self):
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.first_chunk = True
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.executor:
            self.executor.shutdown(wait=True)
        if exception_type:
            print(f"An exception occurred: {exception_value}")
        return True  # Suppress exceptions if any

    def save_to_csv(self, df):
        mode = 'w' if self.first_chunk else 'a'
        df.to_csv(self.csv_file_path, mode=mode, header=self.first_chunk, index=False)
        self.first_chunk = False
        print(f"Saved {len(df)} rows to CSV: {self.csv_file_path}")

    def save_to_hdfs(self, df):
        # Convert to Parquet format and save to HDFS
        client = InsecureClient(self.hdfs_url, user='hdfs')
        with client.write(self.hdfs_path, overwrite=self.first_chunk, append=not self.first_chunk) as writer:
            table = Table.from_pandas(df)
            pq.write_table(table, writer)
        self.first_chunk = False
        print(f"Saved {len(df)} rows to HDFS: {self.hdfs_path}")

    def async_save(self, df):
        if self.save_to_hdfs:
            self.executor.submit(self.save_to_hdfs, df)
        else:
            self.executor.submit(self.save_to_csv, df)
