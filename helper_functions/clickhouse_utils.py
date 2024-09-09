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

def append_and_upsert_df_to_clickhouse(df, table_name, unique_keys, client=None, max_execution_time=300):
    """
    Appends data to a ClickHouse table and performs an upsert operation based on unique keys.
    
    :param df: pandas DataFrame to write
    :param table_name: name of the table in ClickHouse
    :param unique_keys: list of column names that form the unique key
    :param client: ClickHouse client (if None, a new connection will be established)
    :param max_execution_time: maximum execution time in seconds (default: 300)
    """
    if client is None:
        client = connect_to_clickhouse_db()

    # Convert all object columns to strings
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    # Replace NaN, inf, and -inf values with None
    df = df.replace([np.inf, -np.inf, np.nan], None)

    # Check if table exists
    result_table_exists = client.command(f"EXISTS TABLE {table_name}")
    table_exists = result_table_exists == 1

    if not table_exists:
        # Create table if it doesn't exist
        create_table_if_not_exists(client, table_name, df)

    # Create a temporary table for the new data
    temp_table_name = f"{table_name}_temp"
    create_table_if_not_exists(client, temp_table_name, df)

    # Insert data into the temporary table
    client.insert_df(temp_table_name, df)

    # Set max execution time
    client.command(f"SET max_execution_time = {max_execution_time}")

    # Perform the delete operation
    unique_keys_str = ", ".join(unique_keys)
    non_null_conditions = " AND ".join([f"a.{key} IS NOT NULL AND b.{key} IS NOT NULL" for key in unique_keys])
    match_conditions = " AND ".join([f"a.{key} = b.{key}" for key in unique_keys])
    delete_query = f"""
    ALTER TABLE {table_name} DELETE WHERE ({unique_keys_str}) IN (
        SELECT a.{unique_keys_str}
        FROM {table_name} a
        INNER JOIN {temp_table_name} b
        ON {match_conditions}
        WHERE {non_null_conditions}
    )
    """
    delete_result = client.command(delete_query)
    deleted_rows = delete_result.summary.get('deleted_rows', 'Unknown')
    # print(f"Deleted {deleted_rows} rows")
    # print(f"Delete result summary: {delete_result.summary}")

    # Perform the insert operation
    columns_str = ", ".join(df.columns)
    insert_query = f"""
    INSERT INTO {table_name} ({columns_str})
    SELECT {columns_str}
    FROM {temp_table_name}
    """
    insert_result = client.command(insert_query)
    inserted_rows = insert_result.summary.get('written_rows', 'Unknown')
    # print(f"Inserted {inserted_rows} rows")
    # print(f"Insert result summary: {insert_result.summary}")

    # Check the number of rows in the main table
    count_query = f"SELECT COUNT(*) FROM {table_name}"
    row_count = client.command(count_query)
    # Check if row_count is an integer or an object with result_rows
    if isinstance(row_count, int):
        total_rows = row_count
    else:
        total_rows = row_count.result_rows[0][0] if row_count.result_rows else 'Unknown'
    print(f"Total rows in {table_name} after upsert: {total_rows}")
    # print(f"Count result: {row_count}")

    # Drop the temporary table
    client.command(f"DROP TABLE {temp_table_name}")

    print(f"Data appended and upserted to table '{table_name}' successfully.")

    # Return some statistics
    return {
        'deleted_rows': deleted_rows,
        'inserted_rows': inserted_rows,
        'total_rows': total_rows
    }