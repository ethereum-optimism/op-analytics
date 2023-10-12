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
from datetime import datetime, timedelta, timezone

import time

import sys

sys.path.append("../helper_functions")
import pandas_utils as pu

sys.path.pop()

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase


def get_dune_data(
    query_id: int,
    name: str = "my_query_results",
    path: str = "csv_outputs",
    params: list = None,
    num_hours_to_rerun=4,
    performance: str = "medium",
) -> pd.DataFrame:
    """
    Get data via Dune API.
    """
    query = QueryBase(
        name=name,
        query_id=query_id,
        params=params
    )
    logger.info(f"Results available at {query.url()}")

    dotenv.load_dotenv()
    dune = DuneClient(os.environ["DUNE_API_KEY"])
    # get latest
    try:
        results = dune.get_latest_result(query_id)
        submitted_at = results.times.submitted_at
        # execution_started_at = latest.times.execution_started_at
        # execution_ended_at = latest.times.execution_ended_at

        # Get the current time in the same timezone as submitted_at
        current_time = datetime.now(submitted_at.tzinfo)

        # Calculate the time difference between the current time and submitted_at
        time_difference = current_time - submitted_at
    except:
        submitted_at = 0
        time_difference = timedelta(hours=num_hours_to_rerun + 1)

    # print(current_time)

    # if data is recent, pull that
    to_rerun = 0
    if time_difference <= timedelta(hours=num_hours_to_rerun):
        try:
            df = pd.DataFrame(results.result.rows)
        except:
            to_rerun = 1
    else:
        pass

    if (time_difference > timedelta(hours=num_hours_to_rerun)) or (to_rerun == 1):
        results = dune.refresh(query, performance = performance)
        df = pd.DataFrame(results.result.rows)
    else:
        pass

    df["last_updated"] = results.times.submitted_at

    if not os.path.exists(path):
        os.makedirs(path)
    # display(df)
    df.to_csv(f"{path}/{name}.csv", escapechar="\\", index=False)

    logger.info(
        f"✨ Results saved as {path}/{name}.csv, with {len(df)} rows and {len(df.columns)} columns."
    )

    return df

def generate_query_parameter(input, field_name, dtype):
    if dtype == 'text':
        par = QueryParameter.text_type(name=field_name, value=input)
    if dtype == 'number':
        par = QueryParameter.number_type(name=field_name, value=input)
    if dtype == 'date':
        par = QueryParameter.date_type(name=field_name, value=input)
    if dtype == 'list':
        par = QueryParameter.enum_type(name=field_name, value=input)
    return par


def get_dune_data_raw(query_id, perf_var="medium"):
    dotenv.load_dotenv()
    # authentiction with api key
    api_key = os.environ["DUNE_API_KEY"]
    headers = {"X-Dune-API-Key": api_key}
    base_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    params = {
        "performance": perf_var,
    }

    api_url = (
        f"https://api.dune.com/api/v1/query/{query_id}/results?api_key={dune_api_key}"
    )
    df = pd.DataFrame(r.get(api_url).json()["rows"])
    df = df.iloc[df.index.get_loc("rows")]
    updated_at = df.iloc[0]["execution_ended_at"]
    df = df[["result"]]
    return df


def write_dune_api_from_csv(data, table_name, table_description):
    headers = {"X-Dune-Api-Key": os.environ["DUNE_API_KEY"]}
    # Define the HTTP endpoint URL and data to send
    url = "https://api.dune.com/api/v1/table/upload/csv"

    payload = {
        "table_name": table_name,
        "description": table_description,
        "data": str(data),
    }

    response = r.post(url, data=json.dumps(payload), headers=headers)

    print("Response status code:", response.status_code)
    print("Response content:", response.content)


def write_dune_api_from_pandas(df, table_name, table_description):
    # clean column names (replace spaces with _, force lowercase, remove quotes and other characters)
    df = pu.formatted_columns_to_csv_format(df)

    # iterate over columns and convert any columns containing hex values to strings
    for col in df.columns:
        try:
            if (
                df[col].dtype == "O"
                and df[col].apply(lambda x: isinstance(x, str) and x.startswith("0x")).any()
            ):
                df[col] = df[col].astype(str).str.lower()
                df[col] = df[col].replace("nan", np.nan)
        except:
            continue

    # convert pandas dataframe to csv
    data = df.to_csv(index=False)
    # print(data)
    print("table at: dune.oplabspbc.dataset_" + table_name)
    # Write to Dune
    write_dune_api_from_csv(data, table_name, table_description)

def cancel_query(execution_id):
    # get API key
    api_key = os.environ["DUNE_API_KEY"]
    # authentiction with api key
    headers = {"X-Dune-API-Key": api_key}
    execution_id = str(execution_id)
    base_url = f"https://api.dune.com/api/v1/execution/{execution_id}/cancel"
    print(base_url)
    result_response = r.request("POST", base_url, headers=headers)
    print(result_response)

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
