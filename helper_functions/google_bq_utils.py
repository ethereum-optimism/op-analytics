import os
import dotenv
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_utils as pu

dotenv.load_dotenv()

def connect_bq_client(project_id = os.getenv("BQ_PROJECT_ID")):
        # Check if running locally
        is_running_local = os.environ.get("IS_RUNNING_LOCAL", "False").lower() == "true"

        # Set the environment variable to the path of your service account key file
        if is_running_local: #GH Action was weird with this, so forcing the datatype here
                print("Running locally")
                # Path to your local service account key file
                service_account_key_path = os.getenv("BQ_APPLICATION_CREDENTIALS")
                credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
                # pandas_gbq.context.credentials = service_account.Credentials.from_service_account_file(os.getenv("PATH_TO_BQ_CREDS"))
        else: #Can't get the Github Action version to work
                print('not running local')
                # Set the Google Cloud service account key from GitHub secret
                service_account_key = json.loads( os.getenv("BQ_APPLICATION_CREDENTIALS") )
                credentials = service_account.Credentials.from_service_account_info(service_account_key)
                # Set the environment variable to the path of your service account key file
                # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

        client = bigquery.Client(credentials=credentials, project=project_id)
        return client

def write_df_to_bq_table(df, table_id, dataset_id = 'api_table_uploads', write_disposition_in = "WRITE_TRUNCATE", project_id = os.getenv("BQ_PROJECT_ID")):
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
                if (column_name == 'date' or column_name == 'dt' or column_name.endswith('_dt') or column_name.startswith('dt_')):
                        bq_data_type = 'DATETIME' #Handle for datefields
                elif column_type == 'float64':
                        bq_data_type = 'FLOAT64'
                elif column_type == 'int64' or column_type == 'uint64':
                        bq_data_type = 'INTEGER'
                elif column_type == 'datetime64[ns]':
                        bq_data_type = 'DATETIME'
                elif column_type == 'bool':  # Add this condition
                        bq_data_type = 'BOOL'
                elif column_type == 'string':
                        bq_data_type = 'STRING'
                # elif column_type == 'object':
                #         # Attempt to flatten nested data if the column contains arrays or dictionaries
                #         try:
                #                 df = pu.flatten_nested_data(df, column_name)
                #                 continue  # Skip adding the original column to the schema
                #         except ValueError:
                #                 bq_data_type = 'STRING'
                else:
                        bq_data_type = 'STRING'

                schema.append(bigquery.SchemaField(column_name, bq_data_type))

        # Create a job configuration to overwrite the table
        job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition_in,
                schema=schema
        )
        client = connect_bq_client(project_id)
        # Load the DataFrame into BigQuery
        job = client.load_table_from_dataframe(
                df, f"{dataset_id}.{table_id}", job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        print(f"Data loaded successfully to {dataset_id}.{table_id}")


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