# saver_csv.py
import logging
import os
import sys

import pandas as pd
from src.saver import Saver


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(message)s')
formatter.default_msec_format = '%s.%03d'
sh.setFormatter(formatter)
logger.addHandler(sh)


class SaverCSV(Saver):
    """
    Saver implementation for saving data to a CSV file.
    """

    def __init__(self, csv_file_path):
        """
        Initialize the SaverCSV class.

        :param csv_file_path: Path to the CSV file.
        """
        super().__init__()
        self.csv_file_path = csv_file_path

    def save_impl(self, df):
        """
        Save DataFrame to CSV file.

        :param df: DataFrame to save.
        """
        if self.first_chunk:
            if isinstance(self.csv_file_path, str) and '/' in self.csv_file_path:
                dirname = os.path.dirname(self.csv_file_path)
                if dirname:
                    logger.debug(f'try create directory {dirname}')
                    os.makedirs(dirname, exist_ok=True)

        mode = 'w' if self.first_chunk else 'a'
        df.to_csv(self.csv_file_path, mode=mode, header=self.first_chunk, index=False)
        logger.debug(f"Saved {len(df)} rows to CSV: {self.csv_file_path}")

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


    # Save to CSV
    with SaverCSV(csv_file_path='output.csv') as saver:
        for df in generator():
            df_processed = process_chunk(df)
            saver.async_save(df_processed)
