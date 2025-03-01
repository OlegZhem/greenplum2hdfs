import pandas as pd
import os
import pytest
from src.saver_hdfs import SaverHDFS

# Helper function to create a sample DataFrame
def create_sample_dataframe():
    return pd.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })

# Test saving to HDFS (mocked)
def test_save_to_hdfs(mocker):
    hdfs_url = "http://localhost:50070"
    hdfs_path = "/user/hdfs/output.parquet"
    df = create_sample_dataframe()

    # Mock the HDFS client to avoid actual HDFS operations
    mock_client = mocker.patch('hdfs.InsecureClient')
    mock_write = mock_client.return_value.write
    mock_writer = mock_write.return_value.__enter__.return_value

    with SaverHDFS(hdfs_url=hdfs_url, hdfs_path=hdfs_path) as saver:
        saver.async_save(df)
        saver.async_save(df)  # Save twice to test append mode

    # Verify that the HDFS write was called correctly
    assert mock_write.call_count == 2
    mock_write.assert_called_with(hdfs_path, overwrite=mocker.ANY, append=mocker.ANY)

# Run the tests
if __name__ == "__main__":
    pytest.main()