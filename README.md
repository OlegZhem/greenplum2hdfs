
# Greenplum2HDFS

![Python Version](https://img.shields.io/badge/python-3.13%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A test Python project. This project moves data from a simulated Greenplum table to HDFS storage, performing various transformations and calculations along the way.

---

## Environment

### Source: Greenplum Table
- **Table Structure:**
  - `column1`: `float` (normal distribution)
  - `column2`: `float` (odd distribution)
  - `column3`: `string` (alphanumeric characters)
  - `column4`: `date` (timestamp containing several days)
Table contains 200 million rows  

### Target: HDFS Storage
- Data will be stored in HDFS after processing.

---

## Tasks

### 1. Data Transformation
- **Remove duplicates**
- **Remove rows with empty `column3`**
- **Remove rows with timestamps between 1 AM and 3 AM**
- **Replace `column3` values without digits to an empty string**

### 2. Data Aggregation (Per Hour)
- **Number of unique values in `column3`**
- **Mean value of `column1` and `column2`**
- **Median value of `column1` and `column2`**

### 3. Merge Aggregated Data
- Merge aggregated data with the initial dataset by the nearest hour.

### 4. Process Float Columns
- **Create histograms for `column1` and `column2`**
- **Calculate 95% confidence intervals for `column1` and `column2`**

### 5. Store Data to HDFS
- Save the processed data to HDFS.

### 6. Create UI
- Build a lightweight UI to interact with the data processing pipeline.

### 7. 15 Metrics(additional)
- Define 15 unique metrics .

---

## Restrictions

1. **No access to Greenplum:** The solution cannot be tested against an actual Greenplum database.
2. **No access to HDFS:** The solution cannot be tested against an actual HDFS storage system.

---

## Implementation

### Data Transformation

Dask was selected as the engine for data transformation for several reasons:
- It integrates well with Python, enabling parallelization of not only data processing but also statistical calculations using existing Python libraries.
- It offers a straightforward cluster setup on a personal machine, allowing parallel computations with limited resources and without complex configurations.

The data source can be either a CSV file or a Greenplum database. Greenplum has not been tested, so potential issues may arise.
To generate test CSV files, a module named generate_file.py was developed. This module was used to generate files of various sizes, including one with 200 million rows.
Tests were executed through the UI. The results can be saved as CSV or Parquet files on the local machine or in HDFS.
I saved the results in CSV format because there were issues with Parquet, and I chose not to spend time resolving them.
Saving to HDFS has not been tested and may require further refinement.

### Confidence Interval Calculation

To determine the type of data distribution, we calculate statistical moments (mean, standard deviation, skewness, kurtosis).
If skewness and kurtosis are close to 0 (kurtosis - 3), the distribution is likely close to normal, allowing us to use the T-intervals method.
Otherwise, we apply the bootstrap method. This approach is implemented in the UI.

The distribution type can also be estimated by examining the histogram, which is available in the UI. If the histogram visually resembles a bell curve, it may indicate a normal distribution.

Additionally, testing of the one-sample Kolmogorov-Smirnov test was performed (located in test/ks_test.py). However, it was not included in the UI.
While this test can provide a more accurate confidence interval if the sample closely matches a known distribution, if the p-value for all tests is less than 0.05, it’s still advisable to use the bootstrap method.



### Libraries Used
- **[Dask](https://pypi.org/project/dask/)**: Flexible parallel computing library for analytics.
- **[Flask](https://pypi.org/project/Flask/)**: Lightweight WSGI web application framework used to create the UI.

| function            | file                               | method                  |
|---------------------|------------------------------------|-------------------------|
| SQL Queries         | `src/queries.py`                   |                         |
| SQL Queries test    | `test/resources`                   |                         |
| Transformation      | `src/dat_processor_dask.py`        | transform_data_frame    |
| Aggregation         | `src/dat_processor_dask.py`        | aggregate_data_frame    |
| Merge               | `src/dat_processor_dask.py`        | merge_with_aggregated_3 |
| Processing test     | `test/test_data_processor_dask.py` |                         |
| Histogram           | `src/dat_processor_dask.py`        | get_histogram           |
| Comfodense Interval | `src/confidence_interval.py`       |                         |
| UI endpoints        | `flaskApp.py`                      |                         |
| UI templates        | `templates/`                       |                         |


## Dependencies

All dependencies are listed in `requirements.txt`. Install them using:

## Testing

Since access to Greenplum is unavailable, test data is generated and stored in CSV files. Use the `generate_file.py` script to generate test data.
Set file name in script and run it.

## Usage


### 1. Generate test data

open file generate_file.py set file name, number of rows, rows per day
generate test data
```bash
python generate_file.py
```

### 2. Start the Flask UI: 

```bash
python flaskApp.py
```

### 3, Run web browser

http://localhost:5000

go to settings section and: 
- set 'CSV source data file' to name of the generated file
- set greenplum connection settings and table name
- set HDFS settings

 