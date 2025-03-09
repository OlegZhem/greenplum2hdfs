import numpy as np
import pytest
from scipy import stats

from src.confidence_interval import ci_vi_t_interval, ci_via_bootstrap, ci_via_percentile


def test_ci_vi_t_interval():
    # Test with a small dataset
    data = [1, 2, 3, 4, 5]
    lower, upper = ci_vi_t_interval(data)
    print(f'lower: {lower} upper: {upper}')
    assert np.isclose(lower, 1.036, atol=0.01)
    assert np.isclose(upper, 4.963, atol=0.01)

    # Test with a larger dataset
    data = np.random.normal(0, 1, 100)
    lower, upper = ci_vi_t_interval(data)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper

    # Test with a custom confidence level
    lower, upper = ci_vi_t_interval(data, confidence_level=0.99)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper

def test_ci_via_bootstrap():
    # Test with a small dataset
    data = [1, 2, 3, 4, 5]
    lower, upper = ci_via_bootstrap(data, bootstrap_iterations=1000)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper

    # Test with a larger dataset
    data = np.random.normal(0, 1, 100)
    lower, upper = ci_via_bootstrap(data, bootstrap_iterations=1000)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper

    # Test with a custom confidence level and bootstrap iterations
    lower, upper = ci_via_bootstrap(data, confidence_level=0.99, bootstrap_iterations=5000)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper

def test_ci_via_percentile():
    # Test with a small dataset using the 'percentile' method
    data = [1, 2, 3, 4, 5]
    lower, upper, method = ci_via_percentile(data, method='percentile', bootstrap_iterations=1000)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper
    assert method == 'percentile'

    # Test with a larger dataset
    data = np.random.normal(0, 1, 100)
    lower, upper, method = ci_via_percentile(data, bootstrap_iterations=1000)
    print(f'lower: {lower} upper: {upper} method: {method}')
    assert lower < upper

    # Test with a larger dataset using the 'basic' method
    data = np.random.normal(0, 1, 100)
    lower, upper, method = ci_via_percentile(data, method='basic', bootstrap_iterations=1000)
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper
    assert method == 'basic'

    # Test with a custom confidence level and bootstrap iterations using the 'bca' method
    lower, upper, method = ci_via_percentile(data, confidence=0.99, bootstrap_iterations=5000, method='bca')
    print(f'lower: {lower} upper: {upper}')
    assert lower < upper
    assert method == 'bca'

    # Test with an invalid method
    with pytest.raises(ValueError):
        ci_via_percentile(data, method='invalid_method')


# Run the tests
if __name__ == "__main__":
    pytest.main()

