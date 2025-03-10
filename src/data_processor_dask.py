import dask
import dask.dataframe as dd
import dask.array as da
from dask.array.stats import skew, kurtosis
from dask.distributed import Client
from dask.diagnostics import ProgressBar
from distributed import progress
import pandas as pd
import scipy

from sqlalchemy import create_engine, text
from sqlalchemy.sql import select
from sqlalchemy.schema import Table, MetaData

from src.greenplum_connector import get_greenplum_connection_uri

#dask.config.set({"distributed.diagnostics.computations": True})

# Function to check if a string contains digits
def contains_digits(s):
    return any(char.isdigit() for char in s)


def clean_partition(df):
    """Applies all transformations inside a single partition."""
    df = df[df["column3"].notnull() & (df["column3"].str.strip() != "")]

    df["column4"] = dd.to_datetime(df["column4"], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce')
    df = df[~((df["column4"].dt.hour >= 1) & (df["column4"].dt.hour < 3))]

    df["column3"] = df["column3"].apply(lambda x: x if contains_digits(x) else "")

    return df

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

def transform_data_frame2(ddf):
    ddf = ddf.map_partitions(clean_partition, meta=ddf)
    #ddf = ddf.drop_duplicates()
    return ddf

def transform_data_frame3(ddf):
    ddf = ddf.map_partitions(clean_partition, meta=ddf)

    ddf["hour"] = ddf["column4"].dt.floor("h")  # Truncate to the hour
    ddf.shuffle("hour")

    ddf = ddf.map_partitions(lambda df: df.drop_duplicates(), meta=ddf)
    return ddf

def greenplum_load_table_dask(table_name, index_col, npartitions=10, **kwargs):
    connection_uri = get_greenplum_connection_uri(**kwargs)
    ddf = dd.read_sql_table(table_name, con=connection_uri, index_col=index_col, npartitions=npartitions)
    return ddf

def greenplum_load_query_dask_sqlalchemy(sql, npartitions=10, **kwargs):
    connection_uri = get_greenplum_connection_uri(**kwargs)
    ddf = dd.read_sql_query( sql, con=connection_uri, npartitions=npartitions)
    return ddf

def aggregate_data_frame(ddf):
    # Convert datetime column to hourly format
    if  ~('hour' in ddf.columns):
        ddf["hour"] = ddf["column4"].dt.floor("h")  # Truncate to the hour

    # Perform aggregations
    agg_ddf = ddf.groupby("hour").agg({
        "column3": "nunique",  # Count of unique values
        "column1": ["mean", "median"],  # Mean and median for column1
        "column2": ["mean", "median"]  # Mean and median for column2
    })

    # Rename columns for clarity
    agg_ddf.columns = ["unique_values_count", "mean_column1", "median_column1", "mean_column2", "median_column2"]

    return agg_ddf

def merge_with_aggregated_1(ddf, agg_ddf):
    # Ensure 'hour' column exists in original DataFrame
    ddf["hour"] = ddf["column4"].dt.floor("h")

    # Perform a left join on "hour" column
    merged_ddf = ddf.merge(agg_ddf, on="hour", how="left")

    return merged_ddf

def adjust_hour_row(row):
    if row.minute < 30:
        return row.replace(minute=0, second=0, microsecond=0)
    else:
        return (row + pd.Timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


def adjust_hour_partition(df, agg_hours):
    df["base_hour"] = df["column4"].dt.floor("h")
    df["next_hour"] = df["base_hour"] + pd.Timedelta(hours=1)

    # Determine the joining hour based on minute conditions
    df["join_hour"] = df["base_hour"]
    df.loc[df["column4"].dt.minute >= 30, "join_hour"] = df["next_hour"]

    # Ensure next_hour exists in agg_hours, otherwise fallback to base_hour
    df.loc[~df["join_hour"].isin(agg_hours), "join_hour"] = df["base_hour"]

    return df[["column1", "column2", "column3", "column4", "join_hour"]]


def merge_with_aggregated_2(trans_ddf, agg_ddf):
    # Get unique hours from agg_ddf
    agg_hours = agg_ddf["hour"].compute().unique()
    #agg_hours = agg_ddf[["hour"]].drop_duplicates()

    # Adjust hours in trans_ddf
    trans_ddf = trans_ddf.map_partitions(adjust_hour_partition, agg_hours=agg_hours, meta={
        "column1": "f8", "column2": "f8", "column3": "O", "column4": "datetime64[ns]", "join_hour": "datetime64[ns]"
    })

    # Perform the join
    merged_ddf = trans_ddf.merge(agg_ddf, left_on="join_hour", right_on="hour", how="left")

    return merged_ddf

def merge_with_aggregated_3(trans_ddf, agg_ddf):
    if 'hour' in trans_ddf.columns:
        trans_ddf = trans_ddf.drop(columns={ 'hour' })
    trans_ddf["base_hour"] = trans_ddf["column4"].dt.floor("h")
    trans_ddf['adjusted_hour'] = trans_ddf['column4'].apply(adjust_hour_row, meta=('column4', 'datetime64[ns]'))

    # First join on adjusted_hour
    merged_ddf = trans_ddf.merge(agg_ddf, left_on="adjusted_hour", right_on="hour", how="left")

    # Identify rows that did not merge in the first join and remove columns from agg_ddf
    unmerged_rows = merged_ddf[merged_ddf["mean_column1"].isna()]
    unmerged_rows = unmerged_rows.drop(columns=agg_ddf.columns)

    # Perform the second join on base_hour for the unmerged rows
    second_merge = unmerged_rows.merge(agg_ddf, left_on="base_hour", right_on="hour", how="left")

    # Combine the results from both joins
    merged_ddf = merged_ddf[~merged_ddf["mean_column1"].isna()]  # Rows that merged in the first join
    final_ddf = dd.concat([merged_ddf, second_merge], axis=0, ignore_index=True)

    #final_ddf = final_ddf.reset_index(drop=True)  # Drop the existing index
    #final_ddf = final_ddf.assign(unique_id=final_ddf.index.map(lambda x: x + 1))  # Create a unique ID
    #final_ddf = final_ddf.set_index('unique_id')  # Set the unique ID as the index

    return final_ddf

def merge_with_aggregated_4(trans_ddf, agg_ddf):
     # First join condition: Adjust based on minutes
    trans_ddf["base_hour"] = trans_ddf["column4"].dt.floor("h")
    trans_ddf['adjusted_hour'] = trans_ddf['column4'].apply(adjust_hour_row, meta=('column4', 'datetime64[ns]'))

    # First join
    merged_ddf = trans_ddf.merge(agg_ddf, left_on="adjusted_hour", right_on="hour", how="left", suffixes=("", "_joined"))

    # Identify rows that were not joined and remove columns belong to agg_ddf
    unmatched_ddf = merged_ddf[merged_ddf["mean_column1"].isna()].drop(
        columns=[col for col in agg_ddf.columns if col != "hour"]
    )

    # Secondary join based only on the original hour
    unmatched_ddf = unmatched_ddf.merge(agg_ddf, left_on="base_hour", right_on="hour", how="left", suffixes=("", "_fallback"))

    # Fill missing values using the second join
    for col in agg_ddf.columns:
        if col != "hour":
            merged_ddf[col] = merged_ddf[col].fillna(unmatched_ddf[col])

    #merged_ddf = merged_ddf.reset_index(drop=True)  # Drop the existing index
    #merged_ddf = merged_ddf.assign(unique_id=merged_ddf.index.map(lambda x: x + 1))  # Create a unique ID
    #merged_ddf = merged_ddf.set_index('unique_id')  # Set the unique ID as the index

    return merged_ddf

def get_histogram(ddf, column, bins=10):
    da_dask = ddf[column].to_dask_array(lengths=True)
    min_task = da_dask.min()
    max_task = da_dask.max()
    min, max = dask.compute(min_task, max_task)  # Parallel execution
    return da.histogram(da_dask, bins=bins, range=[min, max])

def get_statistic(ddf, column):
    da_dask = ddf[column].to_dask_array(lengths=True)

    # Create lazy computation graphs (nothing is executed yet)
    mean_task = da_dask.mean()
    std_task = da_dask.std(ddof=1)
    skew_task = dask.delayed(skew)(da_dask)  # Skew & kurtosis are not Dask-native, so use dask.delayed
    kurtosis_task = dask.delayed(kurtosis)(da_dask)

    # Compute all metrics in parallel
    mean, std, skew_val, kurtosis_val = dask.compute(mean_task, std_task, skew_task, kurtosis_task)

    return mean, std, skew_val.compute(), kurtosis_val.compute()


 # Usage example

def usage_greenplum():
    GREENPLUM_CONNECTION_PARAMS = {
        'host': 'your_host',
        'port': 'your_port',
        'dbname': 'your_db',
        'user': 'your_user',
        'password': 'your_password'
    }
    DASK_BLOCK_SIZE = "64MB"
    with Client() as client:
        df_dask = greenplum_load_table_dask("my_table", "column4", **GREENPLUM_CONNECTION_PARAMS)
        df_processed = transform_data_frame(df_dask).compute()
        progress(client.persist(df_processed))
    result_df = df_dask.compute()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(result_df)


if __name__ == "__main__":

    column = 'column2'
    df_dask = dd.read_csv('../data/test_data_2M.csv', blocksize='64MB', usecols=[column])
    hist, bins = get_histogram(df_dask, column)
    print(hist.compute())
    print(bins)

    #result_df = df_dask.compute()
    #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #    print(result_df)


