#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import dotenv
import os
dotenv.load_dotenv()
import csv
import json
import requests
import time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urlencode
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import google_bq_utils as bqu
import clickhouse_utils as ch
sys.path.pop()


# In[ ]:


APIS = {
    "ethereum": {
        "url": "https://api.etherscan.io/api",
        "key": os.environ["L1_ETHERSCAN_API"]
    },
    "base": {
        "url": "https://api.basescan.org/api",
        "key": os.environ["BASESCAN_API"]
    },
    "op": {
        "url": "https://api-optimistic.etherscan.io/api",
        "key": os.environ["OP_ETHERSCAN_API"]
    }
}


# In[ ]:


def read_addresses_from_csv(filename: str) -> List[Tuple[str, str, str]]:
    addresses = []
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        for row in reader:
            chain, address, name = row
            addresses.append((chain, address, name))
    return addresses

def get_balance(network: str, address: str) -> float:
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
        "apikey": APIS[network]["key"]
    }
    
    # Construct the full URL with parameters
    full_url = f"{APIS[network]['url']}?{urlencode(params)}"
    
    # Print the full URL for debugging
    # print(f"Debug - API call URL: {full_url}")

    response = requests.get(APIS[network]["url"], params=params)
    # print(response.json())
    if response.status_code == 200:
        result = response.json()
        if result["status"] == "1":
            balance = int(result["result"]) / 1e18  # Convert wei to ETH
            return balance
    return None

def check_balances(addresses: List[Tuple[str, str, str]]) -> Dict[str, float]:
    balances = {}
    for chain, address, name in addresses:
        balance = get_balance(chain, address)
        # print(balance)
        if balance is not None:
            balances[name] = {"balance": balance, "address": address}
        else:
            balances[name] = {"balance": None, "address": address}
        time.sleep(0.1)
    return balances

def get_inflight_withdrawals():
        wds = d.get_dune_data(query_id = 3939869, #https://dune.com/queries/3939869
                name = "l1_to_l2_inflight_withdrawals",
                path = "outputs",
                performance="large",
                num_hours_to_rerun=0 #always rereun
                )
        wds_bal = wds['amount_eth'].sum()
        return wds_bal


# In[ ]:


csv_filename = 'inputs/address_list.csv'  # Make sure this file exists in the same directory as your script
addresses = read_addresses_from_csv(csv_filename)
balances = check_balances(addresses)
balances['l1_to_l2_inflight_withdrawals'] = {"balance": get_inflight_withdrawals(), "address": 'https://dune.com/embeds/3939869/6626683/'} 


# In[ ]:


# Output balances as JSON
json_txt = json.dumps(balances, indent=2)
# print(json_txt)
# Create DataFrame
current_time = datetime.now()
df_data = []
for name, data in balances.items():
    df_data.append({
        'dt': pd.Timestamp(current_time.date()),
        'timestamp': pd.Timestamp(current_time.isoformat()),
        'address_type': name,
        'address': data['address'],
        'balance': data['balance']
    })

df = pd.DataFrame(df_data)


# In[ ]:


table_name = 'op_collective_balances'
# Write JSON to file
with open(f'outputs/latest_{table_name}.json', 'w') as json_file:
        json_file.write(json_txt)
print(f"JSON written")

# # Append DataFrame to CSV
# csv_output_file = f'outputs/{table_name}_history.csv'
# df.to_csv(csv_output_file, mode='a', header=not pd.io.common.file_exists(csv_output_file), index=False)

# print(f"CSV appended")

# Write latest values to a separate CSV, replacing it on every run
latest_csv_file = f'outputs/latest_{table_name}.csv'
df.to_csv(latest_csv_file, index=False)
print(f"Latest values written to {latest_csv_file}")


# In[ ]:


df.dtypes


# In[ ]:


upload_table = f'daily_{table_name}'
#BQ Upload
bqu.append_and_upsert_df_to_bq_table(df, upload_table, unique_keys = ['dt','address_type','address'])
#CH Upload
ch.write_df_to_clickhouse(df, upload_table, if_exists='append')

