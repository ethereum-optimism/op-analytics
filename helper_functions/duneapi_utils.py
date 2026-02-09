# -*- coding: utf-8 -*-
# based off of Cow Protocol's Dune Client: https://github.com/cowprotocol/dune-client
import pandas as pd
import dotenv
import os
# from loguru import logger
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Tuple, Optional, Any

def get_dune_data_crud(query_name, query_str, query_params = None, query_is_private = False):
    dotenv.load_dotenv()
    dune = DuneClient(os.environ["DUNE_API_KEY"])

    query = dune.create_query(
    name=query_name,
    query_sql=query_str,
    # Optional fields
    params=query_params,
    is_private=query_is_private
    )
    query_id = query.base.query_id
    print(f"Created query with id {query.base.query_id}")

    get_dune_data(query_id = query_id, name = query_name, params = query_params)

def get_dune_data(
    query_id: int,
    name: str = "my_query_results",
    path: str = "csv_outputs",
    params: list = None,
    num_hours_to_rerun=4,
    performance: str = "medium",
    save_to_csv: bool = False
) -> pd.DataFrame:
    """
    Get data via Dune API.
    """
    query = QueryBase(
        name=name,
        query_id=query_id,
        params=params
    )
    print(f"Results available at {query.url()}")

    dotenv.load_dotenv()
    dune = DuneClient(os.environ["DUNE_API_KEY"])
    # get latest
    try:
        print('getting latest result')
        results = dune.get_latest_result(query)
        submitted_at = results.times.submitted_at
        # print(f'latest result submitted at {submitted_at}')
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

    df["last_updated"] = pd.to_datetime(results.times.submitted_at)
    df["last_updated"] = df["last_updated"].dt.tz_localize(None)
    df["last_updated"] = df["last_updated"].dt.tz_localize("UTC").dt.tz_convert(None)

    if not os.path.exists(path):
        os.makedirs(path)
    # display(df)
    if save_to_csv:
        df.to_csv(f"{path}/{name}.csv", escapechar="\\", index=False)

        print(
            f"✨ Results saved as {path}/{name}.csv, with {len(df)} rows and {len(df.columns)} columns. Query: {query.url()}"
        )

    return df

def generate_query_parameter(input, field_name, dtype):
    if dtype == 'text':
        par = QueryParameter.text_type(name=field_name, value=input)
    elif dtype == 'number':
        par = QueryParameter.number_type(name=field_name, value=input)
    elif dtype == 'date':
        par = QueryParameter.date_type(name=field_name, value=input)
    elif dtype == 'list':
        par = QueryParameter.enum_type(name=field_name, value=input)
    else:
        print('invalid datatype: ' + dtype)
        par = None
        # print(par)
    return par


def generate_chunk_ranges(
    trailing_days: int,
    chunk_size: int,
    ending_days: int = 0
) -> List[Tuple[int, int]]:
    """
    Generate a list of (days_start, days_end) tuples for chunked date ranges.

    Args:
        trailing_days: Total number of days to cover (e.g., 28)
        chunk_size: Size of each chunk in days (e.g., 3)
        ending_days: Offset from today to stop at (default 0 = today)

    Returns:
        List of (days_start, days_end) tuples, e.g., [(25, 28), (22, 25), ...]
    """
    chunk_ranges = []
    current_end = trailing_days
    while current_end > ending_days:
        days_start = max(current_end - chunk_size, ending_days)
        chunk_ranges.append((days_start, current_end))
        current_end = days_start
    return chunk_ranges


def run_dune_chunked(
    query_id: int,
    trailing_days: int,
    chunk_size: int = 3,
    ending_days: int = 0,
    name: str = "chunked_query",
    path: str = "outputs",
    performance: str = "large",
    extra_params: List = None,
    num_hours_to_rerun: int = 4,
    parallel: bool = True,
    max_workers: int = 3,
    trailing_days_param_name: str = "trailing_days",
    ending_days_param_name: str = "ending_days",
    dedupe_cols: List[str] = None
) -> pd.DataFrame:
    """
    Run a Dune query in chunks to avoid timeout issues.

    This function breaks a large date range query into smaller chunks,
    optionally running them in parallel for faster execution.

    Args:
        query_id: The Dune query ID
        trailing_days: Total number of days to cover
        chunk_size: Size of each chunk in days (default 3)
        ending_days: Offset from today to stop at (default 0)
        name: Name for the query results
        path: Output path for results
        performance: Dune performance tier ("small", "medium", "large")
        extra_params: Additional query parameters to include
        num_hours_to_rerun: Hours before forcing a refresh
        parallel: Whether to run chunks in parallel (default True)
        max_workers: Number of parallel workers (default 3)
        trailing_days_param_name: Name of the trailing days parameter in the query
        ending_days_param_name: Name of the ending/lookback days parameter in the query
        dedupe_cols: Columns to use for deduplication (removes overlapping boundary rows)

    Returns:
        Combined DataFrame from all chunks
    """
    chunk_ranges = generate_chunk_ranges(trailing_days, chunk_size, ending_days)

    def fetch_chunk(days_start: int, days_end: int) -> pd.DataFrame:
        # trailing_days = how far back from NOW() to start (e.g., 25 = 25 days ago)
        # ending_days = how far back from NOW() to end (e.g., 22 = 22 days ago)
        # So for chunk (22, 25): trailing_days=25, ending_days=22 gives range 25-22 days ago
        days_param = generate_query_parameter(
            input=days_end,
            field_name=trailing_days_param_name,
            dtype='number'
        )
        end_days_param = generate_query_parameter(
            input=days_start,
            field_name=ending_days_param_name,
            dtype='number'
        )

        params = [days_param, end_days_param]
        if extra_params:
            params.extend(extra_params)

        return get_dune_data(
            query_id=query_id,
            name=name,
            path=path,
            performance=performance,
            params=params,
            num_hours_to_rerun=num_hours_to_rerun
        )

    dfs = []

    if parallel and len(chunk_ranges) > 1:
        print(f'Running {len(chunk_ranges)} chunks in parallel with {max_workers} workers')
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_chunk, start, end): (start, end)
                for start, end in chunk_ranges
            }
            for future in as_completed(futures):
                start, end = futures[future]
                try:
                    chunk_df = future.result()
                    if not chunk_df.empty:
                        dfs.append(chunk_df)
                    print(f'Completed chunk days {start}-{end}')
                except Exception as e:
                    print(f'Failed chunk days {start}-{end}: {e}')
    else:
        for days_start, days_end in chunk_ranges:
            print(f'Running chunk: day range {days_start}-{days_end}')
            try:
                chunk_df = fetch_chunk(days_start, days_end)
                if not chunk_df.empty:
                    dfs.append(chunk_df)
            except Exception as e:
                print(f'Failed chunk days {days_start}-{days_end}: {e}')

    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        # Remove duplicates from overlapping chunk boundaries (BETWEEN is inclusive in SQL)
        if dedupe_cols:
            before_count = len(result_df)
            result_df = result_df.drop_duplicates(subset=dedupe_cols, keep='first')
            after_count = len(result_df)
            if before_count != after_count:
                print(f'Removed {before_count - after_count} duplicate rows from chunk overlaps')
        return result_df
    return pd.DataFrame()


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

# Function to generate schema from DataFrame
def generate_dune_schema(df):
    schema = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        column_type = None
        nullable = True  # Assume columns are nullable by default
        if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
            column_type = 'double'
        elif pd.api.types.is_bool_dtype(dtype):
            column_type = 'boolean'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            column_type = 'timestamp'
        elif pd.api.types.is_string_dtype(dtype):
            column_type = 'string'
        else:
            raise TypeError(f"Unsupported dtype for column '{column_name}': {dtype}")

        schema.append({
            "name": column_name,
            "type": column_type,
            "nullable": nullable
        })
    return schema

def create_dune_table(data_for_schema, namespace, table_name, table_description, is_private = False):
    # we need to create Dune tables in order to insert in to them (not upload)
    dune = DuneClient(os.environ["DUNE_API_KEY"])

    table = dune.create_table(
        namespace=namespace,
        table_name=table_name,
        description=table_description,
        schema= generate_dune_schema(data_for_schema),
        is_private=is_private
    )
    
    # print("Response status code:", table.status_code)
    # print("Response content:", table)


def insert_dune_api_from_csv(data, table_name, namespace):

    # https://docs.dune.com/api-reference/tables/endpoint/insert
    dune = DuneClient(os.environ["DUNE_API_KEY"])
    print(namespace, table_name)
    # Define the HTTP endpoint URL and data to send
    response = dune.insert_table(
        namespace=namespace,
        table_name=table_name,
        data=data,
        content_type="text/csv"
    )

    print("Response content:", response)


def insert_dune_api_from_pandas(df, table_name, namespace):
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
    # Write to Dune
    print('inserting ' + table_name)
    insert_dune_api_from_csv(data, table_name, namespace)
    print("table at: " +namespace +'.'+ table_name)


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
    print('uploading ' + table_name)
    # Write to Dune
    write_dune_api_from_csv(data, table_name, table_description)
    print("table at: dune.oplabspbc.dataset_" + table_name)

def cancel_query(execution_id):
    # get API key
    api_key = os.environ["DUNE_API_KEY"]
    # authentiction with api key
    headers = {"X-Dune-API-Key": api_key}
    execution_id = str(execution_id)
    base_url = f"https://api.dune.com/api/v1/execution/{execution_id}/cancel"
    # print(base_url)
    result_response = r.request("POST", base_url, headers=headers)
    # print(result_response)

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
