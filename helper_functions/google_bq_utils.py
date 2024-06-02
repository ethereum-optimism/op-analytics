import os
import dotenv
import json
from google.cloud import bigquery
from google.oauth2 import service_account

dotenv.load_dotenv()

def connect_bq_client():
        # Check if running locally
        is_running_local = os.environ.get("IS_RUNNING_LOCAL", "False").lower() == "true"
        print(is_running_local)

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
        project_id = os.getenv("BQ_PROJECT_ID")

        client = bigquery.Client(credentials=credentials, project=project_id)
        return client

def write_df_to_bq_table(df, table_id, dataset_id = 'api_table_uploads', write_disposition_in = "WRITE_TRUNCATE"):
        # Reset the index of the DataFrame to remove the index column
        df = df.reset_index(drop=True)
        
        # Create a job configuration to overwrite the table
        job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition_in
        )
        client = connect_bq_client()
        # Load the DataFrame into BigQuery
        job = client.load_table_from_dataframe(
                df, f"{dataset_id}.{table_id}", job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        print(f"Data loaded successfully to {dataset_id}.{table_id}")