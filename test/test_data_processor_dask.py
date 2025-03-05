import dask.dataframe as dd
import pandas as pd
import numpy as np
from src.data_processor_dask import *
import pytest

def test_remove_duplicates():
    # remove duplicates
    data = {
        'column1': [1, 2, 3, 3],
        'column2': [10, 20, 30, 30],
        'column3': ['abc123', 'def', 'ghi456', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 11:30', '2023-10-01 12:00', '2023-10-01 04:00', '2023-10-01 04:00'])
    }

    pdf = pd.DataFrame(data)
    df = dd.from_pandas(pdf, npartitions=1)
    #df = dd.DataFrame(data)
    processed_df = transform_data_frame(df).compute()
    assert len(processed_df) == 3

def test_filter_time():
    # time filter
    data = {
        'column1': [1, 2],
        'column2': [10, 20],
        'column3': ['abc123', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 01:30', '2023-10-01 04:00'])
    }
    pdf = pd.DataFrame(data)
    df = dd.from_pandas(pdf, npartitions=1)
    # df = dd.DataFrame(data)
    processed_df = transform_data_frame(df)
    assert len(processed_df) == 1

def test_clear_column3():
    # empty column
    data = {
        'column1': [1, 2, 3],
        'column2': [10, 20, 30],
        'column3': ['abc123', 'def', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 11:30', '2023-10-01 12:00', '2023-10-01 04:00'])
    }
    pdf = pd.DataFrame(data)
    df = dd.from_pandas(pdf, npartitions=1)
    # df = dd.DataFrame(data)
    processed_df = transform_data_frame(df).compute()
    assert processed_df['column3'].tolist() == ['abc123', '', 'ghi456']

def test_remove_empty_column3():
    # remove empty column
    data = {
        'column1': [1, 2, 3],
        'column2': [10, 20, 30],
        'column3': ['abc123', '', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 11:30', '2023-10-01 12:00', '2023-10-01 04:00'])
    }
    pdf = pd.DataFrame(data)
    df = dd.from_pandas(pdf, npartitions=1)
    # df = dd.DataFrame(data)
    processed_df = transform_data_frame(df).compute()
    assert len(processed_df) == 2

def test_convert_from_string_column4():
    data = {
        'column1': [1, 2, 3],
        'column2': [10, 20, 30],
        'column3': ['abc123', '4jts', 'ghi456'],
        'column4': ['2023-10-01 11:30', '2023-10-01 12:00', '2023-10-01 04:00']
    }
    pdf = pd.DataFrame(data)
    df = dd.from_pandas(pdf, npartitions=1)
    #df = dd.DataFrame(data)
    processed_df = transform_data_frame(df).compute()
    assert len(processed_df) == 3

@pytest.fixture
def trans_df():
    """Creates a sample Dask DataFrame with 4 different hours and varying row counts."""
    data = {
        "column1": [
            1.0, 2.0, 100.0, 4.0,  # 10:00 AM
            10.0, 20.0, 30.0, 40.0, 90.0,  # 11:00 AM
            5.0, 15.0, 40.0,  # 12:00 PM
            8.0, 25.0  # 01:00 PM
        ],
        "column2": [
            5.0, 10.0, 50.0, 100.0,  # 10:00 AM
            2.0, 8.0, 30.0, 35.0, 90.0,  # 11:00 AM
            1.0, 7.0, 50.0,  # 12:00 PM
            9.0, 30.0  # 01:00 PM
        ],
        "column3": [
            "A", "B", "A", "C",  # 10:00 AM (Unique: A, B, C)
            "D", "E", "D", "F", "G",  # 11:00 AM (Unique: D, E, F, G)
            "H", "I", "J",  # 12:00 PM (Unique: H, I, J)
            "K", "L"  # 01:00 PM (Unique: K, L)
        ],
        "column4": pd.to_datetime([
            "2024-03-02 10:05:00", "2024-03-02 10:15:00", "2024-03-02 10:45:00", "2024-03-02 10:55:00",  # 10:00 AM
            "2024-03-02 11:05:00", "2024-03-02 11:15:00", "2024-03-02 11:25:00", "2024-03-02 11:35:00", "2024-03-02 11:45:00",  # 11:00 AM
            "2024-03-02 12:05:00", "2024-03-02 12:15:00", "2024-03-02 12:25:00",  # 12:00 PM
            "2024-03-02 13:05:00", "2024-03-02 13:15:00"  # 01:00 PM
        ])
    }

    return pd.DataFrame(data)

@pytest.fixture
def agg_df():
    """Creates a sample Aggregation. Mean and median are different"""
    data = {
        "hour": [
            pd.Timestamp("2024-03-02 10:00:00"),
            pd.Timestamp("2024-03-02 11:00:00"),
            pd.Timestamp("2024-03-02 12:00:00"),
            pd.Timestamp("2024-03-02 13:00:00"),
        ],
        "unique_values_count": [3, 4, 3, 2],  # Unique values in column3 per hour
        "mean_column1": [26.75, 38.0, 20.0, 16.5],
        "median_column1": [3.0, 30.0, 15.0, 16.5],
        "mean_column2": [41.250000, 33.00000, 19.333333, 19.50000],
        "median_column2": [30.0, 30.0, 7.0, 19.5],
    }

    return pd.DataFrame(data)


def test_aggregate_data_frame(trans_df, agg_df):
    """Tests the aggregation function with multiple hours."""
    agg_ddf = aggregate_data_frame(dd.from_pandas(trans_df, npartitions=2))

    # Convert to Pandas for testing correctness
    result_df = agg_ddf.compute()
    #print(result_df.to_dict())
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(result_df)

    # Ensure aggregated DataFrame matches expected results
    pd.testing.assert_frame_equal(result_df.reset_index().round(2), agg_df.round(2))

def test_join_data_frames(trans_df, agg_df):
    trans_ddf = dd.from_pandas(trans_df, npartitions=2)
    agg_ddf = dd.from_pandas(agg_df, npartitions=2)

    result_ddf = merge_with_aggregated(trans_ddf, agg_ddf)

    # Convert to Pandas for testing correctness
    result_df = result_ddf.compute()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(result_df)

    assert result_df.shape[0] == 14
    assert len(result_df.columns) == 10

@pytest.fixture
def df_with_float_column():
    data = {
           "column": [
            6.0, 11.0, 51.0, 101.0,  12.0, 18.0, 31.0, 36.0, 91.0,  11.0, 17.0, 56.0,  19.0, 31.0,
            7.0, 12.0, 52.0, 100.0,  22.0, 28.0, 32.0, 37.0, 92.0,  12.0, 27.0, 54.0,  29.0, 32.0,
            5.0, 10.0, 50.0, 100.0,   2.0,  8.0, 30.0, 35.0, 90.0,   1.0,  7.0, 50.0,   9.0, 30.0
        ]}
    return pd.DataFrame(data)

def test_get_histogram(df_with_float_column):
    ddf = dd.from_pandas(df_with_float_column, npartitions=2)
    hist, bins = get_histogram(ddf, "column2")
    print(hist.compute())
    np.testing.assert_array_equal( hist.compute(), np.array([9, 8, 6, 7, 2, 4, 0, 0, 1, 5]))
    print(bins)
    print(type(bins))
    np.testing.assert_array_equal( bins,  np.array([  1.,  11.,  21.,  31.,  41.,  51.,  61.,  71.,  81.,  91., 101.]))

def test_statistic(df_with_float_column):
    ddf = dd.from_pandas(df_with_float_column, npartitions=2)
    mean, std = get_statistic(ddf, "column")
    print(f' mean {type(mean)}: {mean}')
    assert mean == 34.61904761904762
    print(f' std {type(std)}: {std}')
    assert std == 29.12285338989229


# Run the tests
if __name__ == "__main__":
    pytest.main()