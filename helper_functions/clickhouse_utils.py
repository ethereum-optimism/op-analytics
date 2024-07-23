import os
import dotenv
import clickhouse_connect as cc
import requests
import pandas as pd
import json
import numpy as np
import json

dotenv.load_dotenv()
# Get OPLabs DB Credentials
env_ch_host = os.getenv("OP_CLICKHOUSE_HOST")
env_ch_user = os.getenv("OP_CLICKHOUSE_USER")
env_ch_pw = os.getenv("OP_CLICKHOUSE_PW")
env_ch_port = os.getenv("OP_CLICKHOUSE_PORT")

def connect_to_clickhouse_db(
    ch_host=env_ch_host,
    ch_port=env_ch_port,
    ch_user=env_ch_user,
    ch_pw=env_ch_pw
):
    client = cc.get_client(host=ch_host, port=ch_port, username=ch_user, password=ch_pw, secure=True)
    return client

def write_api_data_to_clickhouse(api_url, table_name, client=None, if_exists='replace'):
    """
    Fetches data from an API and writes it to a ClickHouse table.
    Creates the table if it doesn't exist.
    """
    if client is None:
        client = connect_to_clickhouse_db()

    # Fetch data from API
    response = requests.get(api_url)
    data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(data)

    write_df_to_clickhouse(df, table_name, client, if_exists)

def write_csv_to_clickhouse(csv_file_path, table_name, client=None, if_exists='replace'):
    """
    Reads a CSV file and writes its contents to a ClickHouse table.
    Creates the table if it doesn't exist.
    """
    if client is None:
        client = connect_to_clickhouse_db()

    # Read CSV file
    df = pd.read_csv(csv_file_path)

    write_df_to_clickhouse(df, table_name, client, if_exists)

def write_df_to_clickhouse(df, table_name, client=None, if_exists='replace', max_execution_time=300):
    """
    Writes a DataFrame to a ClickHouse table, adding new columns if necessary.
    
    :param df: pandas DataFrame to write
    :param table_name: name of the table in ClickHouse
    :param client: ClickHouse client (if None, a new connection will be established)
    :param if_exists: 'append' to add to existing data, 'replace' to delete existing data and overwrite
    :param max_execution_time: maximum execution time in seconds (default: 300)
    """
    if client is None:
        client = connect_to_clickhouse_db()

    # Replace NaN, inf, and -inf values with None
    df = df.replace([np.inf, -np.inf, np.nan], None)

    # Check if table exists
    result_table_exists = client.command(f"EXISTS TABLE {table_name}")
    table_exists = result_table_exists == 1
    if table_exists:
        if if_exists == 'replace':
            # Delete existing data
            client.command(f"TRUNCATE TABLE {table_name}")
            print(f"Existing data in table '{table_name}' has been deleted.")
        
        # Get existing columns from the ClickHouse table
        existing_columns = client.command(f"DESCRIBE TABLE {table_name}")
        
        # Clean up the existing columns list
        cleaned_columns = [col.strip() for col in existing_columns if col.strip()]
        
        # Parse the existing columns
        existing_column_info = []
        for i in range(0, len(cleaned_columns), 2):  # Each column info takes 2 elements
            column_name = cleaned_columns[i]
            column_type = cleaned_columns[i+1]
            existing_column_info.append((column_name, column_type))

        # Create a dictionary mapping column names to their ClickHouse types
        column_type_map = dict(existing_column_info)

        # Convert columns to their expected types based on ClickHouse schema
        for col in df.columns:
            if col in column_type_map:
                if column_type_map[col].startswith('Nullable(String)'):
                    df[col] = df[col].astype(str)
                elif column_type_map[col].startswith('Nullable(Int64)'):
                    df[col] = df[col].astype('Int64')
                elif column_type_map[col].startswith('Nullable(Float64)'):
                    df[col] = df[col].astype('float64')
                elif column_type_map[col].startswith('Nullable(DateTime64)'):
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)  # Ensure datetime without timezone

        # Identify new columns in the DataFrame
        existing_column_names = list(column_type_map.keys())
        new_columns = [col for col in df.columns if col not in existing_column_names]

        # Add new columns to the ClickHouse table
        for column in new_columns:
            ch_type = get_clickhouse_type(df[column].dtype)
            client.command(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `{column}` {ch_type}")
            print(f"Added new column '{column}' with type '{ch_type}' to table '{table_name}'")

    else:
        # Create table if it doesn't exist
        create_table_if_not_exists(client, table_name, df)

    # Write to ClickHouse with increased max_execution_time
    client.command(f"SET max_execution_time = {max_execution_time}")
    
    # Use the insert_df method which handles datetime objects correctly
    client.insert_df(table_name, df)
    
    if if_exists == 'append':
        print(f"Data appended to table '{table_name}' successfully.")
    else:
        print(f"Data written to table '{table_name}' successfully, replacing previous data.")

# Define the get_clickhouse_type function
def get_clickhouse_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "Nullable(Int64)"
    elif pd.api.types.is_float_dtype(dtype):
        return "Nullable(Float64)"
    elif pd.api.types.is_bool_dtype(dtype):
        return "Nullable(UInt8)"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "Nullable(DateTime64(3))"
    else:
        return "Nullable(String)"

# Define the create_table_if_not_exists function
def create_table_if_not_exists(client, table_name, df):
    columns = []
    for col, dtype in df.dtypes.items():
        ch_type = get_clickhouse_type(dtype)
        columns.append(f"`{col}` {ch_type}")
    
    columns_str = ", ".join(columns)
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_str}
    ) ENGINE = MergeTree() ORDER BY tuple()
    """
    client.command(query)
    print(f"Table '{table_name}' created or already exists.")