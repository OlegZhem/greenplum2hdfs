import numpy as np
from scipy import stats

def ci_vi_t_interval(data, confidence_level=0.95):
    data = np.array(data)
    n = len(data)
    mean = np.mean(data)
    std = np.std(data, ddof=1)
    degrees_of_freedom = n - 1
    t_value = stats.t.ppf(1 - (1 - confidence_level) / 2, degrees_of_freedom)
    margin_of_error = t_value * (std / np.sqrt(n))
    return mean - margin_of_error, mean + margin_of_error

def ci_via_bootstrap(data, confidence_level=0.95, bootstrap_iterations=10000):
    data = np.array(data)
    n = len(data)
    bootstrap_means = [np.mean(np.random.choice(data, size=n, replace=True)) for _ in
                       range(bootstrap_iterations)]
    lower_bound = np.percentile(bootstrap_means, (1 - confidence_level) / 2 * 100)
    upper_bound = np.percentile(bootstrap_means, (1 + confidence_level) / 2 * 100)
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
