import dask.dataframe as dd

# Function to check if a string contains digits
def contains_digits(s):
    return any(char.isdigit() for char in s)

def transform_data_frame(df):
    # Remove rows where column3 is empty
    df = df[df["column3"].notnull() & (df["column3"].str.strip() != "")]

    # Remove rows where column4 (timestamp) is between 1 AM and 3 AM
    df["column4"] = dd.to_datetime(df["column4"], format='%Y-%m-%d %H:%M:%S:%f', errors='coerce')  # Ensure column4 is datetime
    df = df[~((df["column4"].dt.hour >= 1) & (df["column4"].dt.hour < 3))]

    # If column3 does not contain digits, set it to an empty string
    df["column3"] = df["column3"].apply(lambda x: x if contains_digits(x) else "", meta=('column3', 'object'))

    # Remove duplicates while keeping one instance
    df = df.drop_duplicates()

    return df

