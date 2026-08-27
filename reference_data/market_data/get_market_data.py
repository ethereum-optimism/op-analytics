#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Generate static txt files for data points that we can reference elsewhere (i.e. Google Sheets)

# Data Points: L1 Base Fee, Blob Base Fee, ETH/USD Conversion
import requests
import time
from dotenv import load_dotenv

load_dotenv()
import os
from datetime import datetime
import pandas as pd

import sys

sys.path.append("../../helper_functions")
import google_bq_utils as bqu

sys.path.pop()

etherscan_api_key = os.environ.get("L1_ETHERSCAN_API")
max_attempts = 3


def get_eth_usd(api_key):
    # Etherscan V2 API
    api_url = (
        f"https://api.etherscan.io/v2/api?chainid=1&module=stats&action=ethprice&apikey={api_key}"
    )
    for attempt in range(max_attempts):
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                print(data)
                ethusd = data.get("result", {}).get("ethusd")
                if ethusd:
                    print(f"ETH Price in USD: {ethusd}")
                    return ethusd
                else:
                    raise ValueError("ETH price in USD not found in the response.")
            else:
                raise Exception("Request failed.")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_attempts - 1:
                print("Retrying in 3 seconds...")
                time.sleep(3)
            else:
                print("Failed to fetch data after 3 attempts.")
                return "0"


def get_suggest_base_fee(api_key):
    # Etherscan V2 API
    api_url = f"https://api.etherscan.io/v2/api?chainid=1&module=gastracker&action=gasoracle&apikey={api_key}"
    for attempt in range(max_attempts):
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                print(data)
                suggest_base_fee = data.get("result", {}).get("suggestBaseFee")
                if suggest_base_fee:
                    print(f"Suggested Base Fee: {suggest_base_fee}")
                    return suggest_base_fee
                else:
                    raise ValueError("Suggested Base Fee not found in the response.")
            else:
                print("Failed to fetch data from Etherscan API for gas oracle.")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_attempts - 1:
                print("Retrying in 3 seconds...")
                time.sleep(3)
            else:
                print("Failed to fetch data after 3 attempts.")
                return "0"


# In[4]:


def get_blob_base_fee_per_gas(rpc_url):
    payload = {"jsonrpc": "2.0", "method": "eth_blobBaseFee", "params": [], "id": 1}
    for attempt in range(max_attempts):
        try:
            # Send a POST request to the RPC endpoint
            response = requests.post(rpc_url, json=payload, timeout=10)

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                result = data.get("result")

                if result and isinstance(result, str) and result.startswith("0x"):
                    # Convert hex wei to decimal gwei
                    blob_base_fee_wei = int(result, 16)
                    blob_base_fee_gwei = blob_base_fee_wei / 1e9

                    # Convert to decimal string with 10 decimal places
                    blob_base_fee_decimal = "{:.10f}".format(blob_base_fee_gwei)
                    return blob_base_fee_decimal
                else:
                    raise ValueError(f"Invalid RPC response: {data}")
            else:
                raise ValueError(f"RPC request failed with status {response.status_code}")

        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_attempts - 1:
                print("Retrying in 3 seconds...")
                time.sleep(3)
            else:
                print("Failed to fetch data after 3 attempts.")
                return "0.0000000000"


# Use a reliable public RPC as Blocknative is shutting down
# LogicNodes was suggested as a replacement but was unreachable during implementation
api_url = "https://ethereum-rpc.publicnode.com"


def safe_float_convert(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert {value} to float. Returning 0.0")
        return 0.0


if __name__ == "__main__":
    current_timestamp = datetime.now()

    # Get Data
    ethusd = get_eth_usd(etherscan_api_key)
    suggest_base_fee = get_suggest_base_fee(etherscan_api_key)
    blob_base_fee = get_blob_base_fee_per_gas(api_url)

    # Write to Endpoints
    os.makedirs("outputs", exist_ok=True)
    with open("outputs/ethusd.txt", "w") as file:
        file.write(str(ethusd))
    with open("outputs/suggest_base_fee.txt", "w") as file:
        file.write(str(suggest_base_fee))
    with open("outputs/blob_base_fee.txt", "w") as file:
        file.write(str(blob_base_fee))

    ethusd = safe_float_convert(ethusd)
    suggest_base_fee = safe_float_convert(suggest_base_fee)
    blob_base_fee = safe_float_convert(blob_base_fee)

    # Upload to BQ
    data = {
        "timestamp": [current_timestamp],
        "eth_usd": [ethusd],
        "l1_base_fee_gwei": [suggest_base_fee],
        "blob_base_fee_gwei": [blob_base_fee],
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame(data)
    print(df)
    print(df.dtypes)
    print(df.columns)
    table_name = "market_data"
    dataset_name = "rpc_table_uploads"
    bqu.write_df_to_bq_table(df, table_id=table_name, dataset_id=dataset_name, write_mode="append")
