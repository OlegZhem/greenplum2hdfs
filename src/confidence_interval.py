import dask
import numpy as np
from scipy import stats

def ci_via_t_interval(data, confidence_level=0.95):
    data = np.array(data)
    n = len(data)
    mean = np.mean(data)
    std = np.std(data, ddof=1)
    degrees_of_freedom = n - 1
    t_value = stats.t.ppf(1 - (1 - confidence_level) / 2, degrees_of_freedom)
    margin_of_error = t_value * (std / np.sqrt(n))
    return mean - margin_of_error, mean + margin_of_error

def ci_via_t_interval_dask(da_dask, confidence_level=0.95):
    n = da_dask.size
    mean_task = da_dask.mean()
    std_task = da_dask.std(ddof=1)
    mean, std = dask.compute(mean_task, std_task)
    degrees_of_freedom = n - 1
    t_value = stats.t.ppf(1 - (1 - confidence_level) / 2, degrees_of_freedom)
    margin_of_error = t_value * (std / np.sqrt(n))
    return mean - margin_of_error, mean + margin_of_error

def ci_via_bootstrap(data, confidence_level=0.95, bootstrap_iterations=1000):
    data = np.array(data)
    n = len(data)
    bootstrap_means = [np.mean(np.random.choice(data, size=n, replace=True)) for _ in range(bootstrap_iterations)]
    lower_bound = np.percentile(bootstrap_means, (1 - confidence_level) / 2 * 100)
    upper_bound = np.percentile(bootstrap_means, (1 + confidence_level) / 2 * 100)
    return lower_bound, upper_bound

def bootstrap_sample(data, num_samples=10000):
    """Generates bootstrap mean estimates for a single partition."""
    n = data.shape[0]
    if n == 0:
        return np.array([])  # Handle empty partitions gracefully

    bootstrap_means = np.mean(np.random.choice(data, size=(num_samples, n), replace=True), axis=1)
    return bootstrap_means

def ci_via_bootstrap_dask(da_dask, confidence_level=0.95, bootstrap_iterations=100):
    # Apply bootstrap sampling in parallel on each partition
    bootstrap_means_delayed = da_dask.map_blocks(
        bootstrap_sample, da_dask, bootstrap_iterations, dtype=np.float64
    )

    # Compute all bootstrap samples in parallel
    bootstrap_means = bootstrap_means_delayed.compute()  # Triggers parallel computation

    # Compute confidence interval percentiles
    alpha = (1 - confidence_level) / 2
    lower_bound = np.percentile(bootstrap_means, alpha * 100)
    upper_bound = np.percentile(bootstrap_means, (1 - alpha) * 100)

    return lower_bound, upper_bound


def ci_via_percentile(data, confidence=0.95, bootstrap_iterations=10000, method=None):
    data = np.array(data)
    n = len(data)

    mean = np.mean(data)

    # Auto-selection of method
    if method is None:
        skewness = stats.skew(data)
        kurt = stats.kurtosis(data)
        if abs(skewness) < 0.5 and abs(kurt - 3) < 1:
            method = 'percentile'
        elif abs(skewness) < 1 and abs(kurt - 3) < 2:
            method = 'basic'
        else:
            method = 'bca'

    bootstrap_means = np.array(
        [np.mean(np.random.choice(data, size=n, replace=True)) for _ in range(bootstrap_iterations)])

    alpha = (1 - confidence) / 2

    if method == 'basic':
        lower_bound = 2 * mean - np.percentile(bootstrap_means, (1 - alpha) * 100)
        upper_bound = 2 * mean - np.percentile(bootstrap_means, alpha * 100)
    elif method == 'percentile':
        lower_bound = np.percentile(bootstrap_means, alpha * 100)
        upper_bound = np.percentile(bootstrap_means, (1 - alpha) * 100)
    elif method == 'bca':
        from scipy.stats import norm
        z0 = norm.ppf((np.sum(bootstrap_means < mean)) / bootstrap_iterations)
        z_alpha = norm.ppf(alpha)
        z_1_alpha = norm.ppf(1 - alpha)
        lower_bound = np.percentile(bootstrap_means, norm.cdf(2 * z0 + z_alpha) * 100)
        upper_bound = np.percentile(bootstrap_means, norm.cdf(2 * z0 + z_1_alpha) * 100)
    else:
        raise ValueError("Invalid method. Choose from 'basic', 'percentile', or 'bca'.")

    return lower_bound, upper_bound, method
