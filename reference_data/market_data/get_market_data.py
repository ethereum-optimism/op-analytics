#!/usr/bin/env python
# coding: utf-8

# In[6]:


# Generate static txt files for data points that we can reference elsewhere (i.e. Google Sheets)

# Data Points: L1 Base Fee, Blob Base Fee, ETH/USD Conversion
import requests
from dotenv import load_dotenv
load_dotenv()
import os

etherscan_api_key = os.environ.get('L1_ETHERSCAN_API')


# In[7]:


# Get ETH/USD
api_url = f"https://api.etherscan.io/api?module=stats&action=ethprice&apikey={etherscan_api_key}"
# Make the GET request
response = requests.get(api_url)
# Check if the request was successful
if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Assuming you want to print or use the ETH price in USD
        ethusd = data.get("result", {}).get("ethusd")
        print(f"ETH Price in USD: {ethusd}")
else:
        print("Failed to fetch data from Etherscan API.")


# In[8]:


api_url_gas_oracle = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={etherscan_api_key}"
response_gas_oracle = requests.get(api_url_gas_oracle)
if response_gas_oracle.status_code == 200:
        # Parse the JSON response
        data_gas_oracle = response_gas_oracle.json()
        # Extract the suggestBaseFee
        suggest_base_fee = data_gas_oracle.get("result", {}).get("suggestBaseFee")
        print(f"Suggested Base Fee: {suggest_base_fee}")
else:
        print("Failed to fetch data from Etherscan API for gas oracle.")


# In[9]:


#Write to Endpoints
with open(f"outputs/ethusd.txt", 'w') as file:
        file.write(ethusd)
with open(f"outputs/suggest_base_fee.txt", 'w') as file:
        file.write(suggest_base_fee)

