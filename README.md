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
1. Transform data
    - remove duplicates
    - remove rows with empty column3
    - remove rows from 1AM to 3 AM
    - replace column3 without digits to empty string
2. Agregate data per hour
    - number of unique column3
    - mean value
    - median value
3. Process float columns
 - create gistogram
 - calculate 95 percent confidence interval
4. Store data to HDFS
5. Create UI

# Restrictions
1. I have no acces to greenplum, so I have no abbility to test solution for greenplan.
2. I have no acces to HDFS, so I have no abbility to test solution for HDFS.

# Assumptions
column4 store timestamp and contains several days. 

# Implementation

## Dependencies
```
pip numpy
```

## Test Data
Due to I have no access to greenplum, I use big csv file for testing.
Generate test dta with generate_file.py. It generates test_data.csv. Set number of rows, number of rows per day and start date.

