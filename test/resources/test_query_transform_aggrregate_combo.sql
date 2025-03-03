CREATE TABLE your_table (
    column1 FLOAT,
    column2 FLOAT,
    column3 VARCHAR(50),
    column4 TIMESTAMP
);
INSERT INTO your_table (column1, column2, column3, column4) VALUES
-- Transformations
(1.23, 4.56, 'abc123', '2023-10-01 03:00:00'),
(2.34, 5.67, 'def456', '2023-10-01 01:30:00'),
(3.45, 6.78, '', '2023-10-03 04:00:00'),
(3.45, 6.78, null, '2023-10-03 04:00:00'),
(4.56, 7.89, 'jklrtj', '2023-10-01 02:30:00'),
(5.67, 8.90, 'mno345', '2023-10-01 05:00:00'),
(5.67, 8.90, 'mno345', '2023-10-01 05:00:00'),
(2.33, 1.12, 'mnoddd', '2023-10-01 15:00:00'),
(2.13, 3.32, 'mno1ddd', '2023-10-01 15:00:00'),
(6.78, 9.01, 'pqr678', '2023-10-01 01:00:00'),
(7.89, 0.12, 'stu901', '2023-10-01 06:00:00'),
(7.89, 0.12, 'stu901', '2023-10-01 06:00:00'),
(7.89, 0.12, 'stu901', '2023-10-01 06:00:00'),
(8.90, 1.23, 'vwx234', '2023-10-01 02:30:00'),
-- Aggregation
-- Hour 1: 3 rows (odd number of rows), skewed data for column1 and column2
(1.0, 10.0, 'abc123', '2023-10-01 11:00:00'),  -- Low value
(1.5, 10.5, 'def456', '2023-10-01 11:15:00'),  -- Medium value
(10.0, 20.0, 'abc123', '2023-10-01 11:30:00'), -- High value (skew)

-- Hour 2: 4 rows (even number of rows), skewed data for column1 and column2
(2.0, 20.0, 'ghi789', '2023-10-01 12:00:00'),  -- Low value
(2.5, 20.5, 'jkl012', '2023-10-01 12:15:00'),  -- Medium value
(3.0, 21.0, 'ghi789', '2023-10-01 12:30:00'),  -- Medium value
(15.0, 30.0, 'jkl012', '2023-10-01 12:45:00'), -- High value (skew)

-- Hour 3: 5 rows (odd number of rows), skewed data for column1 and column2
(3.0, 30.0, 'mno345', '2023-10-01 03:00:00'),  -- Low value
(3.5, 30.5, 'pqr678', '2023-10-01 03:15:00'),  -- Medium value
(4.0, 31.0, 'mno345', '2023-10-01 03:30:00'),  -- Medium value
(4.5, 31.5, 'stu901', '2023-10-01 03:45:00'),  -- Medium value
(20.0, 40.0, 'vwx234', '2023-10-01 03:50:00'), -- High value (skew)

-- Hour 4: 2 rows (even number of rows), no skew (mean = median)
(4.0, 40.0, 'yz1234', '2023-10-01 04:00:00'),
(6.0, 60.0, 'ab5678', '2023-10-01 04:30:00');