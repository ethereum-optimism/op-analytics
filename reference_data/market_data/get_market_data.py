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

etherscan_api_key = os.environ.get('L1_ETHERSCAN_API')
max_attempts = 3


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


#Write to Endpoints
with open(f"outputs/ethusd.txt", 'w') as file:
        file.write(ethusd)
with open(f"outputs/suggest_base_fee.txt", 'w') as file:
        file.write(suggest_base_fee)

