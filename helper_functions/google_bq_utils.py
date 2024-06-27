import os
import dotenv
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_utils as pu

dotenv.load_dotenv()

import logging

# Setup logging configuration
logging.basicConfig(level=logging.INFO)  # Set logging level to INFO
logger = logging.getLogger(__name__)  # Create logger instance for this module


def connect_bq_client(project_id = os.getenv("BQ_PROJECT_ID")):
        # Check if running locally
        is_running_local = os.environ.get("IS_RUNNING_LOCAL", "False").lower() == "true"

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

def check_table_exists(client, table_id, dataset_id='api_table_uploads', project_id=os.getenv("BQ_PROJECT_ID")):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        return True
    except Exception as e:
        if 'Not found' in str(e):
            return False
        else:
            raise e

def get_bq_type(column_name, column_type):
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
    elif column_type == 'string':
        return 'STRING'
    else:
        return 'STRING'
        
def write_df_to_bq_table(df, table_id, dataset_id = 'api_table_uploads'
                         ,  write_mode='overwrite'
                         , project_id = os.getenv("BQ_PROJECT_ID")):
        print(f"Start Writing {dataset_id}.{table_id}")
        schema = []
        # Reset the index of the DataFrame to remove the index column
        df = df.reset_index(drop=True)

        # Check for any flattens to do
        for column_name, column_type in df.dtypes.items():
                if column_type == 'object':
                        # Attempt to flatten nested data if the column contains arrays or dictionaries
                        try:
                                df = pu.flatten_nested_data(df, column_name)
                                continue  # Skip adding the original column to the schema
                        except ValueError:
                                continue
        for column_name, column_type in df.dtypes.items():
                # Map pandas data types to BigQuery data types
                bq_data_type = get_bq_type(column_name, column_type)

                schema.append(bigquery.SchemaField(column_name, bq_data_type))

        # Set the write disposition based on the append_or_update parameter
        if write_mode == 'append':
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif write_mode == 'overwrite':
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
              print('Error: Must be append or overwrite')
        # Create a job configuration to overwrite the table
        job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema=schema
        )
        client = connect_bq_client(project_id)
        # Load the DataFrame into BigQuery
        job = client.load_table_from_dataframe(
                df, f"{dataset_id}.{table_id}", job_config=job_config
        )

        # Wait for the job to complete
        try:
                job.result()  # Wait for the job to complete
                print(f"Data loaded successfully to {dataset_id}.{table_id}")
        except Exception as e:
                print(f"Error loading data to BigQuery: {e}")
                # Log more details if needed
                raise  # Re-raise the exception for higher-level error handling

def append_and_upsert_df_to_bq_table(df, table_id, dataset_id='api_table_uploads', project_id=os.getenv("BQ_PROJECT_ID"), unique_keys=['chain', 'dt']):
    client = connect_bq_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        # Check if the table exists
        if check_table_exists(client, table_id, dataset_id, project_id):
            # Get the existing table schema
            table = client.get_table(table_ref)
            existing_columns = set(field.name for field in table.schema)
            
            # Identify new columns
            new_columns = set(df.columns) - existing_columns
            
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
    client = connect_bq_client(project_id)

    # Run the query and get the results as a pandas DataFrame
    query_job = client.query(query)
    results = query_job.result().to_dataframe()

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