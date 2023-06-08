# -*- coding: utf-8 -*-
# based off of Cow Protocol's Dune Client: https://github.com/cowprotocol/dune-client
import pandas as pd
import dotenv
import os
from loguru import logger
import argparse
import requests as r
import re
import json
import numpy as np

import sys
sys.path.append("../helper_functions")
import pandas_utils as pu
sys.path.pop()

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import Query


def get_dune_data(
    query_id: int, name: str = "my_query_results", path: str = "csv_outputs", performance: str = "medium"
) -> pd.DataFrame:
    """
    Get data via Dune API.
    """
    query = Query(
        name=name,
        query_id=query_id,
    )
    logger.info(f"Results available at {query.url()}")

    dotenv.load_dotenv()
    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh(query)
    performance = performance

    df = pd.DataFrame(results.result.rows)
    df["last_updated"] = results.times.submitted_at

    if not os.path.exists(path):
        os.makedirs(path)
    # display(df)
    df.to_csv(f"{path}/{name}.csv", escapechar='\\', index=False)

    logger.info(
        f"✨ Results saved as {path}/{name}.csv, with {len(df)} rows and {len(df.columns)} columns."
    )

    return df

def get_dune_data_raw(query_id, perf_var = "medium"):
    dotenv.load_dotenv()
    # authentiction with api key
    api_key = os.environ["DUNE_API_KEY"]
    headers = {"X-Dune-API-Key": api_key}
    base_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    params = {
        "performance": perf_var,
    }


    api_url = f"https://api.dune.com/api/v1/query/{query_id}/results?api_key={dune_api_key}"
    df = pd.DataFrame(r.get(api_url).json()['rows'])
    df = df.iloc[df.index.get_loc('rows')]
    updated_at = df.iloc[0]['execution_ended_at']
    df = df[['result']]
    return df


def write_dune_api_from_csv(data, table_name, table_description):
    headers = {'X-Dune-Api-Key': os.environ["DUNE_API_KEY"]}
    # Define the HTTP endpoint URL and data to send
    url = 'https://api.dune.com/api/v1/table/upload/csv'

    payload = {
        "table_name": table_name,
        "description": table_description,
        "data": str(data)
    }
    
    response = r.post(url, data=json.dumps(payload), headers=headers)

    print('Response status code:', response.status_code)
    print('Response content:', response.content)

def write_dune_api_from_pandas(df, table_name, table_description):
    #clean column names (replace spaces with _, force lowercase, remove quotes and other characters)
    df = pu.formatted_columns_to_csv_format(df)

    #iterate over columns and convert any columns containing hex values to strings
    for col in df.columns:
        if df[col].dtype == 'O' and df[col].apply(lambda x: isinstance(x, str) and x.startswith('0x')).any():
            df[col] = df[col].astype(str).str.lower()
            df[col] = df[col].replace('nan', np.nan)

    #convert pandas dataframe to csv
    data = df.to_csv(index=False)
    print(data)
    # Write to Dune
    write_dune_api_from_csv(data, table_name, table_description)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Call Dune API and pull data from a query. Enter the query_id and name of the file you want to save as."
    )
    parser.add_argument("--query_id", type=int, help="Enter query_id")
    parser.add_argument(
        "--name", type=str, help="Name of the query, which will also be saved as csv."
    )
    parser.add_argument("--path", type=str, help="Path of the csv to be saved.")

    args = parser.parse_args()

    get_dune_data(args.query_id, args.name, args.path)
