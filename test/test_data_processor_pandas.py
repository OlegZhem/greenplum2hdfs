# tests/test_transformer.py
import pandas as pd
from src.data_processor_pandas import transform_data_frame
import pytest

def test_remove_duplicates():
    # remove duplicates
    data = {
        'column1': [1, 2, 3, 3],
        'column2': [10, 20, 30, 30],
        'column3': ['abc123', 'def', 'ghi456', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 11:30', '2023-10-01 12:00', '2023-10-01 04:00', '2023-10-01 04:00'])
    }
    df = pd.DataFrame(data)
    processed_df = transform_data_frame(df)
    assert len(processed_df) == 3

def test_filter_time():
    # time filter
    data = {
        'column1': [1, 2],
        'column2': [10, 20],
        'column3': ['abc123', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 01:30', '2023-10-01 04:00'])
    }
    df = pd.DataFrame(data)
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
    df = pd.DataFrame(data)
    processed_df = transform_data_frame(df)
    assert processed_df['column3'].tolist() == ['abc123', '', 'ghi456']

def test_remove_empty_column3():
    # remove empty column
    data = {
        'column1': [1, 2, 3],
        'column2': [10, 20, 30],
        'column3': ['abc123', '', 'ghi456'],
        'column4': pd.to_datetime(['2023-10-01 11:30', '2023-10-01 12:00', '2023-10-01 04:00'])
    }
    df = pd.DataFrame(data)
    processed_df = transform_data_frame(df)
    assert len(processed_df) == 2