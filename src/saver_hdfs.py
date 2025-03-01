# saver_hdfs.py
from pyarrow import parquet as pq
from pyarrow import Table
from hdfs import InsecureClient
from src.saver import Saver


class SaverHDFS(Saver):
    """
    Saver implementation for saving data to HDFS in Parquet format.
    """

    def __init__(self, hdfs_url, hdfs_path):
        """
        Initialize the SaverHDFS class.

        :param hdfs_url: URL for the HDFS server.
        :param hdfs_path: Path in HDFS where the file will be saved.
        """
        super().__init__()
        self.hdfs_url = hdfs_url
        self.hdfs_path = hdfs_path
        self.storage = f"HDFS: {hdfs_url} {hdfs_path}"

    def save_impl(self, df):
        """
        Save DataFrame to HDFS in Parquet format.

        :param df: DataFrame to save.
        """
        client = InsecureClient(self.hdfs_url, user='hdfs')
        try:
            with client.write(self.hdfs_path, overwrite=self.first_chunk, append=not self.first_chunk) as writer:
                table = Table.from_pandas(df)
                pq.write_table(table, writer)
            print(f"Saved {len(df)} rows to {self.storage}")
        except Exception as e:
            print(f"Failed to save to HDFS: {e}")
            raise

if __name__ == "__main__":

    # Usage example
    def process_chunk(df):
        """
        Placeholder function for processing a DataFrame chunk.
        """
        return df


    def generator():
        """
        Placeholder function for generating DataFrame chunks.
        """
        for _ in range(10):
            yield pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})


    # Save to HDFS
    with SaverHDFS(
            hdfs_url='http://your_hdfs_url:50070',
            hdfs_path='hdfs://path/to/your/hdfs/file.parquet'
    ) as saver:
        for df in generator():
            df_processed = process_chunk(df)
            saver.async_save(df_processed)
