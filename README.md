# greenplum2hdfs
Python project to move data from one storage to another with transformation

# Environment
1. greenplum table with 200 millions rows
 Table structure:
      - column1 - float - normal distribution
      - column2 - float - random distribution
      - column3 - string - alfabatic chars and digits
      - column4 - date 
2. HDFS storage

# Tasks
1. Transform data by program and by SQL
    - remove duplicates
    - remove rows with empty column3
    - remove rows from 1AM to 3 AM
    - replace column3 without digits to empty string
2. Aggregate data per hour by programm and by SQL
    - number of unique values in column3
    - mean value in column1 and column2
    - median value in column1 and column2
3. Merge Aggregated data with initial data set by neearst hour.
4. Process float columns
 - create histogram
 - calculate 95 percent confidence interval
5. Store data to HDFS
6. Create UI
7. Calculte 10 metrics

# Restrictions
1. I have no access to greenplum, so I have no ability to test solution for greenplum.
2. I have no access to HDFS, so I have no ability to test solution for HDFS.

# Assumptions
column4 store timestamp and contains several days. 

# Implementation

## Dependencies
```
pip numpy
```

## Test Data
Due to I have no access to greenplum, I use big csv file for testing.
Generate test data with generate_file.py. Set file name, number of rows, number of rows per day and start date.

