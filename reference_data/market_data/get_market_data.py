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

etherscan_api_key = os.environ.get('L1_ETHERSCAN_API')
max_attempts = 3


# In[2]:


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


# In[3]:


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


# In[4]:


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


# In[5]:


#Write to Endpoints
with open(f"outputs/ethusd.txt", 'w') as file:
        file.write(ethusd)
with open(f"outputs/suggest_base_fee.txt", 'w') as file:
        file.write(suggest_base_fee)
with open(f"outputs/blob_base_fee.txt", 'w') as file:
        file.write(blob_base_fee)

