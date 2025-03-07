import datetime

import dask.dataframe as dd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats
import seaborn as sns
import io
import base64

from src.ui_properties import settings


def fetch_data(column):
    df_dask = dd.read_csv(settings["IN_CSV_FILE_PATH"], blocksize=settings["DASK_BLOCK_SIZE"], usecols=[column])
    return df_dask


# Find best-fit distribution
def find_best_fit_distribution(data):
    dist_names = ['norm', 'expon', 'lognorm', 'gamma', 'beta', 'weibull_min']
    best_fit = None
    best_p = 0
    for dist_name in dist_names:
        dist = getattr(stats, dist_name)
        params = dist.fit(data)
        print(f'{dist_name} params {params}')
        statistic, p_value = stats.kstest(data, dist_name, args=params)
        print(f'method: {dist_name} statistic: {statistic} p value: {p_value}')
        if p_value > best_p:
            best_p = p_value
            best_fit = dist_name
    return best_fit


# Compute 95% confidence interval based on best-fit distribution
def confidence_interval(data, best_fit):
    dist = getattr(stats, best_fit)
    params = dist.fit(data)
    ci = dist.interval(0.95, *params)
    return best_fit, ci


# Generate histogram plot and return base64 string
def generate_histogram(data, column_name):
    plt.figure(figsize=(6, 4))
    sns.histplot(data, bins=50, kde=True, color='blue')
    plt.title(f"Histogram of {column_name}")
    plt.xlabel("Value")
    plt.ylabel("Frequency")

    return plt


if __name__ == "__main__":
    column = "column1"
    df = fetch_data(column).compute()
    print(f" column  count: {len(df[column])}")

    best_fit = find_best_fit_distribution(df[column])

    if(best_fit is not None):
        ci = confidence_interval(df[column], best_fit)

        histogram = generate_histogram(df[column], column)

        print(f"best_fit: {best_fit}, 95% CI: {ci}")
        histogram.show()
    else:
        print("best_fit absent")

