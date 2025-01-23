import os
import dotenv
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import pandas_utils as pu
import pandas as pd
import math
import subprocess

dotenv.load_dotenv()

import logging

# Setup logging configuration
logging.basicConfig(level=logging.ERROR)  # Set logging level to ERROR
logger = logging.getLogger(__name__)  # Create logger instance for this module

def setup_google_cloud_env():
    """
    Set up Google Cloud environment variables if not already set.
    Returns (success, error_message) tuple
    """
    # Default paths
    default_creds_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    
    # Check if credentials are already set
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        if os.path.exists(default_creds_path):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = default_creds_path
        else:
            return False, f"No credentials found at {default_creds_path}"
    
    # Check if project is already set
    if not os.getenv('GOOGLE_CLOUD_PROJECT'):
        try:
            # First try to get project from credentials file
            with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) as f:
                creds_data = json.load(f)
                if 'quota_project_id' in creds_data:
                    os.environ['GOOGLE_CLOUD_PROJECT'] = creds_data['quota_project_id']
                elif 'project_id' in creds_data:
                    os.environ['GOOGLE_CLOUD_PROJECT'] = creds_data['project_id']
                else:
                    # If not in credentials, try getting from gcloud config
                    try:
                        result = subprocess.run(
                            ['gcloud', 'config', 'get-value', 'project'],
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        project = result.stdout.strip()
                        if project:
                            os.environ['GOOGLE_CLOUD_PROJECT'] = project
                        else:
                            return False, "No project found in gcloud config"
                    except subprocess.CalledProcessError as e:
                        return False, f"Error getting project from gcloud: {str(e)}"
                    except FileNotFoundError:
                        return False, "gcloud command not found"
        except (json.JSONDecodeError, FileNotFoundError) as e:
            return False, f"Error reading credentials file: {str(e)}"
    
    return True, None

def connect_bq_client(project_id = os.getenv("BQ_PROJECT_ID")):
        # Check if running in a GCP environment and will use the default credentials
        # -------------- start OIDC login
        # In this case the enviroment variables already contain the credentials set up by the GCP login performed
        try:
            logging.info("Using OIDC login")
            #project_id is taken from the environment variables GOOGLE_CLOUD_PROJECT
            client=bigquery.Client()
            return client
        except Exception as e:
            logging.error(f"Exception occurred while trying to use OIDC login")

        try: 
            logging.info("Using local machine credentials")
            setup_google_cloud_env()
            client = bigquery.Client(project=project_id)
            return client
        except Exception as e:
            logging.error(f"Exception occurred while trying to get logging configuration file")
            return None
        # -------------- end OIDC login

        # Check if running locally
        is_running_local = os.environ.get("IS_RUNNING_LOCAL", "False").lower() == "true"

        try:
            # Using try-except block to catch any exceptions that may occur and suppress the error message

            # Set the environment variable to the path of your service account key file
            if is_running_local: #GH Action was weird with this, so forcing the datatype here
                    # print("Running locally")
                    # Path to your local service account key file
                    service_account_key_path = os.getenv("BQ_APPLICATION_CREDENTIALS")
                    credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
            else: #Can't get the Github Action version to work
                    # print('not running local')
                    # Set the Google Cloud service account key from GitHub secret
                    service_account_key = json.loads( os.getenv("BQ_APPLICATION_CREDENTIALS") )
                    credentials = service_account.Credentials.from_service_account_info(service_account_key)

            client = bigquery.Client(credentials=credentials, project=project_id)
            return client
        except Exception as e:
            logging.critical(f"Exception occurred while trying to get logging configuration file")
            return None

def check_table_exists(client, table_id, dataset_id='api_table_uploads', project_id=os.getenv("BQ_PROJECT_ID")):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        return True
    except Exception as e:
        if 'Not found' in str(e):
            return False
        else:
            logger.error("An error occurred while checking table existence")
            raise

def validate_schema(df, schema):
    df_columns = set(df.columns)
    schema_columns = set(field.name for field in schema)
    if df_columns != schema_columns:
        print("DataFrame columns:", df_columns)
        print("Schema columns:", schema_columns)
        missing = schema_columns - df_columns
        extra = df_columns - schema_columns
        raise ValueError(f"Schema mismatch. Missing: {missing}, Extra: {extra}")
    
def get_bq_type(column_name, column_type, series):
    column_name = str(column_name)
    column_type = str(column_type)

    if pu.is_repeated_field(series):
        if series.apply(lambda x: isinstance(x, dict)).any():
            return 'RECORD'
        else:
            return 'STRING'  # For lists, we'll store as JSON strings
    
    if (column_name == 'date' or column_name == 'dt' or 
        column_name.endswith('_dt') or column_name.startswith('dt_')):
        return 'DATETIME'
    elif column_type == 'float64':
        return 'FLOAT64'
    elif column_type in ['int64', 'uint64']:
        return 'INTEGER'
    elif column_type in ['Int64', 'UInt64']:
        return 'FLOAT64'  # Or 'INTEGER' if you prefer
    elif column_type == 'datetime64[ns]':
        return 'DATETIME'
    elif column_type == 'bool':
        return 'BOOL'
    elif column_type in('string','python[string]'):
        return 'STRING'
    else:
        return 'STRING'
    
def clean_chain_id(value):
    if pd.isna(value):
        return ''
    
    # Convert to string and remove '.0' if present
    str_value = str(value).strip()
    return str_value[:-2] if str_value.endswith('.0') else str_value

def drop_table_if_exists(client, project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.delete_table(table_ref)
        print(f"Table {table_ref} deleted.")
    except NotFound:
        print(f"Table {table_ref} does not exist.")
        
def write_df_to_bq_table(df, table_id, dataset_id='api_table_uploads', 
                         write_mode='overwrite', project_id=os.getenv("BQ_PROJECT_ID"), 
                         chunk_size=100000):
    print(f"Start Writing {dataset_id}.{table_id}")
    
    client = connect_bq_client(project_id)

    # Check if the table exists
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        table = client.get_table(table_ref)
        # Use existing schema if table exists
        schema = table.schema
    except NotFound:
        # Create schema based on the first chunk if table doesn't exist
        first_chunk = df.iloc[:chunk_size]
        schema = create_schema(first_chunk)

    first_chunk = df.iloc[:chunk_size]
    
    # Set the write disposition
    write_disposition = (bigquery.WriteDisposition.WRITE_APPEND if write_mode == 'append' 
                         else bigquery.WriteDisposition.WRITE_TRUNCATE)

    # Create a job configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema
    )

    # Calculate the number of chunks
    total_rows = len(df)
    num_chunks = math.ceil(total_rows / chunk_size)

    for i, (_, chunk_df) in enumerate(df.groupby(df.index // chunk_size)):
        # Reset index for each chunk
        chunk_df = chunk_df.reset_index(drop=True)
        # Ensure chain id isn't weird
        try:
            for col in df.columns:
                if ('chain_id' in col.lower() or 'chainid' in col.lower()) and (df[col].dtype == 'object' or df[col].dtype == 'string'):
                    chunk_df[col] = chunk_df[col].apply(clean_chain_id)
        except Exception as e:
            print(f"An error occurred while processing column {col}: {str(e)}")
                
        # Process the chunk (flatten nested data, etc.)
        # print("Original DataFrame columns:", df.columns.tolist())
        # print("Chunk columns before processing:", chunk_df.columns.tolist())
        chunk_df = process_chunk(chunk_df)
        # print("Chunk columns after processing:", chunk_df.columns.tolist())
        # print("Schema fields:", [field.name for field in schema])

        validate_schema(chunk_df, schema)
        # Load the chunk into BigQuery
        job = client.load_table_from_dataframe(
            chunk_df, f"{dataset_id}.{table_id}", job_config=job_config
        )

        try:
            job.result()  # Wait for the job to complete
            print(f"Chunk {i+1}/{num_chunks} loaded successfully to {dataset_id}.{table_id}")
        except Exception as e:
            print(f"Error loading chunk {i+1}/{num_chunks} to BigQuery: {e}")
            raise  # Re-raise the exception for higher-level error handling

        # If it's not the first chunk, change write mode to append
        if i == 0 and write_mode == 'overwrite':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    print(f"All data loaded successfully to {dataset_id}.{table_id}")

def create_schema(df):
    # print('makin schema')
    schema = []
    for column_name, column_type in df.dtypes.items():
        bq_data_type = get_bq_type(column_name, column_type, df[column_name])
        if bq_data_type == 'RECORD':
            schema.append(bigquery.SchemaField(column_name, bq_data_type, mode='REPEATED'))
        else:
            schema.append(bigquery.SchemaField(column_name, bq_data_type))
    # print(schema)
    return schema

def process_chunk(df):
    original_columns = df.columns.tolist()
    processed_df = df.copy()

    for column_name, column_type in df.dtypes.items():
        if pu.is_repeated_field(df[column_name]):
            processed_df[column_name] = df[column_name].apply(lambda x: json.dumps(x) if x is not None else None)
        elif column_type == 'object':
            try:
                processed_df = pu.flatten_nested_data(processed_df, column_name)
            except ValueError:
                continue

    # Ensure all original columns are present
    for col in original_columns:
        if col not in processed_df.columns:
            processed_df[col] = df[col]  # Restore the original column

    # Reorder columns to match original order
    processed_df = processed_df[original_columns]

    return processed_df

def append_and_upsert_df_to_bq_table(df, table_id, dataset_id='api_table_uploads', project_id=os.getenv("BQ_PROJECT_ID"), unique_keys=['chain', 'dt']):
    client = connect_bq_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    for key in unique_keys:
        if key in df.columns:
            try:
                df[key] = df[key].fillna('none')
            except Exception as e:
                print(f"Warning: Could not fill NULLs for column {key}. Error: {str(e)}")
    
    try:
        # Check if the table exists
        if check_table_exists(client, table_id, dataset_id, project_id):
            # Get the existing table schema
            table = client.get_table(table_ref)
            existing_columns = set(field.name for field in table.schema)
            
            # Identify new columns
            new_columns = set(df.columns) - existing_columns

            # After checking for new columns
            missing_columns = existing_columns - set(df.columns)
            if missing_columns:
                for col in missing_columns:
                    df[col] = None  # or an appropriate default value
            
            # If there are new columns, alter the table
            if new_columns:
                new_schema = table.schema[:]
                for new_col in new_columns:
                    bq_data_type = get_bq_type(new_col, str(df[new_col].dtype))
                    new_schema.append(bigquery.SchemaField(new_col, bq_data_type))
                
                table.schema = new_schema
                client.update_table(table, ['schema'])
                logger.info(f"Added new columns to {table_id}: {', '.join(new_columns)}")
                
            # Create staging table for upsert
            staging_table_id = f"{table_id}_staging"
            staging_table_ref = f"{project_id}.{dataset_id}.{staging_table_id}"
            
            # Write data to staging table (overwrite mode)
            # print(df.head(5))
            delete_bq_table(table_id = staging_table_id, dataset_id = dataset_id, project_id=project_id)
            write_df_to_bq_table(df, staging_table_id, dataset_id, write_mode='overwrite', project_id=project_id)
            
            # Perform upsert from staging table to main table
            merge_query = f"""
            MERGE `{table_ref}` T
            USING `{staging_table_ref}` S
            ON {" AND ".join([f"T.{key} = S.{key}" for key in unique_keys])}
            WHEN MATCHED THEN
              UPDATE SET {", ".join([f"T.{col} = S.{col}" for col in df.columns if col not in unique_keys])}
            WHEN NOT MATCHED THEN
              INSERT ({", ".join(df.columns)}) VALUES ({", ".join([f'S.{col}' for col in df.columns])})
            """
            
            # Execute the merge query
            query_job = client.query(merge_query)
            query_job.result()
            
            logger.info(f"Append and upsert to {dataset_id}.{table_id} completed successfully")
            
            # Clean up staging table
            client.delete_table(staging_table_ref)
            delete_bq_table(table_id = staging_table_id, dataset_id = dataset_id, project_id=project_id)
            logger.info(f"Staging table {staging_table_ref} deleted.")
            
        else:
            # If the table doesn't exist, just create it by writing the data (overwrite mode)
            write_df_to_bq_table(df, table_id, dataset_id, write_mode='overwrite', project_id=project_id)
    
    except Exception as e:
        logger.error(f"Error during append_and_upsert_df_to_bq_table: {e}")
        raise  # Re-raise the exception for higher-level error handling

# WARNING THE DELETES TABLES
def delete_bq_table(dataset_id, table_id, project_id=os.getenv("BQ_PROJECT_ID")):
    """
    Deletes a table from a BigQuery dataset.

    Args:
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to be deleted.
        project_id (str, optional): The ID of the Google Cloud project. If not provided,
            the project ID from the environment variable GOOGLE_CLOUD_PROJECT will be used.

    Returns:
        bool: True if the table was deleted successfully, False otherwise.
    """
    client = connect_bq_client(project_id = project_id)

    # Get the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        # Delete the table
        client.delete_table(table_ref, not_found_ok=True)
        print(f"Table '{table_id}' deleted successfully from dataset '{dataset_id}'.")
        return True
    except Exception as e:
        print(f"Error deleting table '{table_id}' from dataset '{dataset_id}': {e}")
        return False

def run_query_to_df(query, project_id=os.getenv("BQ_PROJECT_ID")):
    client = connect_bq_client(project_id)
    # Run the query and get the results as a pandas DataFrame
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    return results
     
     
def run_query_and_save_csv(query, destination_file, project_id=os.getenv("BQ_PROJECT_ID")):
    """
    Runs a BigQuery SQL query and saves the results to a CSV file.

    Args:
        query (str): The SQL query to execute.
        destination_file (str): The path and filename for the CSV file to be created.
        project_id (str, optional): The BigQuery project ID. Defaults to the value of the BQ_PROJECT_ID environment variable.

    Returns:
        None
    """
    results = run_query_to_df(query, project_id)

    # Save the DataFrame to a CSV file
    results.to_csv(destination_file, index=False)
    print(f"Query results saved to {destination_file}")


def remove_duplicates(table_id, dataset_id='api_table_uploads', project_id=os.getenv("BQ_PROJECT_ID"), unique_keys=['chain', 'date']):
    client = connect_bq_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Create a temporary table to hold the deduplicated data
    temp_table_id = f"{table_id}_deduped_temp"
    temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"

    # SQL query to remove duplicates
    dedup_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_ref}` AS
    SELECT
        {', '.join([f'{col}' for col in client.get_table(table_ref).schema if col.name != 'row_num_dedup'])}
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {', '.join(unique_keys)} ORDER BY {unique_keys[0]}) AS row_num_dedup
        FROM
            `{table_ref}`
    )
    WHERE
        row_num_dedup = 1
    """

    # Execute the deduplication query
    dedup_job = client.query(dedup_query)
    dedup_job.result()
    print(f"Temporary deduplicated table {temp_table_ref} created.")

    # Replace the original table with the deduplicated table
    replace_query = f"""
    CREATE OR REPLACE TABLE `{table_ref}` AS
    SELECT
        *
    FROM
        `{temp_table_ref}`
    """

    replace_job = client.query(replace_query)
    replace_job.result()
    print(f"Original table {table_ref} replaced with deduplicated data.")

    # Drop the temporary table
    client.delete_table(temp_table_ref)
    print(f"Temporary table {temp_table_ref} deleted.")
