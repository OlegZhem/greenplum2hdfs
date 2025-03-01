import pandas as pd
import os
import pytest
from src.saver_csv import SaverCSV

# Helper function to create a sample DataFrame
def create_sample_dataframe():
    return pd.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })

# Test saving to CSV
def test_save_to_csv(tmpdir):
    csv_file_path = tmpdir.join("output.csv")
    df = create_sample_dataframe()

    with SaverCSV(csv_file_path=str(csv_file_path)) as saver:
        saver.async_save(df)
        saver.async_save(df)  # Save twice to test append mode

    # Verify the CSV file
    saved_df = pd.read_csv(csv_file_path)
    expected_df = pd.concat([df, df], ignore_index=True)
    pd.testing.assert_frame_equal(saved_df, expected_df)

# Test exception handling
def test_exception_handling(tmpdir, mocker):
    csv_file_path = tmpdir.join("output.csv")
    df = create_sample_dataframe()

    # Mock an exception in the executor
    mocker.patch('concurrent.futures.ThreadPoolExecutor.submit', side_effect=Exception("Test exception"))

    with SaverCSV(csv_file_path=str(csv_file_path)) as saver:
        saver.async_save(df)

    # Verify that the exception was handled and the file was not created
    assert not os.path.exists(csv_file_path)

# Test context manager
def test_context_manager(tmpdir):
    csv_file_path = tmpdir.join("output.csv")
    df = create_sample_dataframe()

    with SaverCSV(csv_file_path=str(csv_file_path)) as saver:
        saver.async_save(df)

    # Verify that the executor was shut down
    assert saver.executor._shutdown

# Run the tests
if __name__ == "__main__":
    pytest.main()