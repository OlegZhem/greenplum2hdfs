CREATE TABLE your_table (
    column1 FLOAT,
    column2 FLOAT,
    column3 VARCHAR(50),
    column4 TIMESTAMP
);
INSERT INTO your_table (column1, column2, column3, column4) VALUES
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