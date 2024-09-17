#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

etherscan_api_key = os.environ.get('L1_ETHERSCAN_API')
max_attempts = 3
current_timestamp = datetime.now()


# In[ ]:


# Get ETH/USD
api_url = f"https://api.etherscan.io/api?module=stats&action=ethprice&apikey={etherscan_api_key}"
# Make the GET request
for attempt in range(max_attempts):
        try:
                response = requests.get(api_url)
                # Check if the request was successful
                if response.status_code == 200:
                        # Parse the JSON response
                        data = response.json()
                        # Assuming you want to print or use the ETH price in USD
                        print(data)
                        ethusd = data.get("result", {}).get("ethusd")
                        if ethusd:
                                print(f"ETH Price in USD: {ethusd}")
                                break  # Exit the loop if the data is successfully fetched and valid
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


# In[ ]:


api_url_gas_oracle = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={etherscan_api_key}"
for attempt in range(max_attempts):
        try:
                response_gas_oracle = requests.get(api_url_gas_oracle)
                if response_gas_oracle.status_code == 200:
                        # Parse the JSON response
                        data_gas_oracle = response_gas_oracle.json()
                        print(data_gas_oracle)
                        # Extract the suggestBaseFee
                        suggest_base_fee = data_gas_oracle.get("result", {}).get("suggestBaseFee")
                        if suggest_base_fee:
                                print(f"Suggested Base Fee: {suggest_base_fee}")
                                break  # Exit the loop if the data is successfully fetched and valid
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


# In[ ]:


def get_blob_base_fee_per_gas(api_url):
    for attempt in range(max_attempts):
        try:
            # Send a GET request to the API
            response = requests.get(api_url)
            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                
                # Ensure the response is for the Ethereum main network
                if data.get('system') == 'ethereum' and data.get('network') == 'main':
                    # Extract the blobBaseFeePerGas from the first block price object
                    blob_base_fee_per_gas = data['blockPrices'][0].get('blobBaseFeePerGas', None)
                    
                    if blob_base_fee_per_gas is not None:
                        # Convert scientific notation to decimal
                        blob_base_fee_per_gas_decimal = "{:.10f}".format(blob_base_fee_per_gas)
                        return blob_base_fee_per_gas_decimal
                    else:
                        raise ValueError("blobBaseFeePerGas not found.")
                else:
                    raise ValueError("blobBaseFeePerGas not found.")
            else:
                raise ValueError("blobBaseFeePerGas not found.")
            
        except Exception as e:
                print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < max_attempts - 1:
                        print("Retrying in 3 seconds...")
                        time.sleep(3)
                else:
                        print("Failed to fetch data after 3 attempts.")

# API URL
api_url = 'https://api.blocknative.com/gasprices/blockprices'

# Call the function and print the result
blob_base_fee  = get_blob_base_fee_per_gas(api_url)


# In[ ]:


# In[ ]:


def safe_float_convert(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert {value} to float. Returning 0.0")
        return 0.0
    
ethusd = safe_float_convert(ethusd)
suggest_base_fee = safe_float_convert(suggest_base_fee)
blob_base_fee = safe_float_convert(blob_base_fee)


# In[ ]:


# Upload to BQ
# Get the current timestamp
# Create a dictionary with your data
# try:
data = {
    'timestamp': [current_timestamp],
    'eth_usd': [ethusd],
    'l1_base_fee_gwei': [suggest_base_fee],
    'blob_base_fee_gwei': [blob_base_fee]
}

# Create a DataFrame from the dictionary
df = pd.DataFrame(data)
print(df)
print(df.dtypes)
print(df.columns)
table_name = 'market_data'
dataset_name = 'rpc_table_uploads'
bqu.write_df_to_bq_table(df, table_id = table_name, dataset_id = 'rpc_table_uploads', write_mode = 'append')
# except Exception as e:
#     print(f"An error occurred: {str(e)}")

