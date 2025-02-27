import logging
import os
import sys

import numpy as np
import random
import string
from datetime import datetime, timedelta

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
sh.setFormatter(formatter)
logger.addHandler(sh)

def generate_random_string(length, with_digits=True):
    """Generate a random string of a given length."""
    if with_digits:
        characters = string.ascii_letters + string.digits
    else:
        characters = string.ascii_letters
    return ''.join(random.choice(characters) for _ in range(length))

def generate_row(current_date):
    """Generate a single row of data."""
    # Generate column1: normally distributed number between 25 and 135
    column1 = np.random.normal(loc=80, scale=20)
    column1 = max(25.0, min(column1, 135.0))  # Clamp values to the range [25, 135]

    # Generate column2: normally distributed number between 5 and 45 + random number between 15 and 35
    column2 = np.random.normal(loc=25, scale=7)
    column2 = max(5.0, min(column2, 45.0)) + 15.0 + 20.0 * random.random()

    # Generate column3: random string with 75% chance of containing digits and letters
    if random.random() < 0.75:
        column3 = generate_random_string(random.randint(7, 15), with_digits=True)
    else:
        column3 = generate_random_string(random.randint(7, 15), with_digits=False)

    # Format column4 as a timestamp string
    column4 = current_date.strftime("%Y-%m-%d %H:%M:%S")

    return [column1, column2, column3, column4]

def generate_and_save_data(num_rows, rows_per_day, start_date, filename):
    """Generate data and save it to a file."""
    current_date = start_date
    # Calculate time increment to evenly distribute rows across the day
    millisecond_per_day = 24 * 3600 * 1000 / rows_per_day
    time_increment = timedelta(milliseconds=millisecond_per_day)
    logger.info(f"time increment {time_increment}")

    if isinstance(filename, str) and '/' in filename:
        dirname = os.path.dirname(filename)
        if dirname:
            logger.info(f'create directory {dirname}')
            os.makedirs(dirname, exist_ok=True)
    with open(filename, 'w') as file:
        logger.info(f"open file {file_name}")
        file.write('column1,column2,column3,column4\n')
        for _ in range(num_rows):
            # Generate a row of data
            row = generate_row(current_date)
            file.write(','.join(map(str, row)) + '\n')

            # Decide whether to add duplicates
            rand = random.random()
            if rand < 0.06:  # 6% chance: 1 duplicate (2 rows total)
                file.write(','.join(map(str, row)) + '\n')
            elif rand < 0.08:  # 2% chance: 2 duplicates (3 rows total)
                file.write(','.join(map(str, row)) + '\n')
                file.write(','.join(map(str, row)) + '\n')

            # Increment the timestamp
            current_date += time_increment

        logger.info(f"file {file_name} generated")

if __name__ == "__main__":
    num_rows = 2_000_000  # Total number of rows to generate
    rows_per_day = 100_000  # Number of rows per day
    start_date = datetime(2025, 1, 1, 0, 0, 0)  # Start date and time (midnight)
    file_name = 'data/test_data.csv'
    # Generate data and save to file
    generate_and_save_data(num_rows, rows_per_day, start_date, file_name)
    logger.info(f"File {file_name} successfully created. Generated {num_rows} rows.")