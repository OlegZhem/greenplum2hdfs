import pandas as pd

# Function to check if a string contains digits
def contains_digits(s):
    return any(char.isdigit() for char in s)


def transform_data_frame(df):
    # Remove rows where column3 is empty
    df = df[df["column3"].notna() & (df["column3"].str.strip() != "")]

    # Remove rows where column4 (timestamp) is between 1 AM and 3 AM
    df = df[~((df["column4"].dt.hour >= 1) & (df["column4"].dt.hour < 3))]

    # If column3 does not contain digits, set it to an empty string
    df["column3"] = df["column3"].apply(lambda x: x if contains_digits(x) else "")

    # Remove duplicates while keeping one instance
    df.drop_duplicates(inplace=True)

    return df


