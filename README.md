
# Greenplum2HDFS

![Python Version](https://img.shields.io/badge/python-3.13%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A test Python project. This project moves data from a simulated Greenplum table to HDFS storage, performing various transformations and calculations along the way.

---

## Environment

### Source: Greenplum Table
- **Table Structure:**
  - `column1`: `float` (normal distribution)
  - `column2`: `float` (random distribution)
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

### 7. Calculate 10 Metrics
- Define and calculate 10 key metrics for the dataset.

---

## Restrictions

1. **No access to Greenplum:** The solution cannot be tested against an actual Greenplum database.
2. **No access to HDFS:** The solution cannot be tested against an actual HDFS storage system.

---

## Assumptions

- `column4` contains timestamps spanning multiple days.
- Test data is generated and stored in CSV files for local testing.

---

## Implementation

### Libraries Used
- **[Pandas](https://pypi.org/project/pandas/)**: Used for data analysis and performance comparison.
- **[Dask](https://pypi.org/project/dask/)**: Flexible parallel computing library for analytics. Main candidate for transformation and aggregation.
- **[Flask](https://pypi.org/project/Flask/)**: Lightweight WSGI web application framework used to create the UI.

### SQL Queries
- SQL queries are stored in `src/queries.py`.

---

## Dependencies

All dependencies are listed in `requirements.txt`. Install them using:

## Test Data

Since access to Greenplum is unavailable, test data is generated and stored in CSV files. Use the `generate_file.py` script to generate test data.
Set file name in script and run it.

## Usage


### 1. Generate test data

```bash
python generate_file.py
```

### 2. Start the Flask UI: 

```bash
python flaskApp.py
```

### 3, Run web browser

http://localhost:5000

 