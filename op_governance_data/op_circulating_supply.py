#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.append("../helper_functions")
import duneapi_utils as du
import google_bq_utils as bqu
sys.path.pop()

# Define the CSV path and API URL
csv_path = 'outputs/daily_op_circulating_supply.csv'  # Update this path as per your directory structure
api_url = 'https://static.optimism.io/tokenomics/circulatingSupply.txt'


# In[ ]:


def update_csv_with_api_data(csv_path, api_url):
    # Read the existing CSV file
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"File {csv_path} not found.")
        return

    # Get the current data from the API
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        current_supply = float(response.text.strip())
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return

    # Get today's date in the required format
    today = datetime.now().strftime('%Y-%m-%d')

    # Check if today's date is already in the DataFrame
    if today in df['date'].values:
        df.loc[df['date'] == today, 'op_circulating_supply'] = current_supply
    else:
        # Backfill missing dates
        last_date_in_csv = datetime.strptime(df['date'].iloc[-1], '%Y-%m-%d')
        delta = datetime.strptime(today, '%Y-%m-%d') - last_date_in_csv
        for i in range(1, delta.days + 1):
            new_date = last_date_in_csv + timedelta(days=i)
            new_date_str = new_date.strftime('%Y-%m-%d')
            df = pd.concat([df, pd.DataFrame({'date': [new_date_str], 'op_circulating_supply': [current_supply]})], ignore_index=True)

    # Save the updated DataFrame back to CSV
    df.to_csv(csv_path, index=False)
    print(f"CSV file updated successfully at {csv_path}")


# In[ ]:


# Call the function to update the CSV
update_csv_with_api_data(csv_path, api_url)


# In[ ]:


try:
    # Read the CSV file
    df = pd.read_csv(csv_path)

except FileNotFoundError:
    print(f"File {csv_path} not found.")
except Exception as e:
    print(f"An error occurred: {e}")


# In[ ]:


# Convert to timestamp
df['date'] = pd.to_datetime(df['date'])
df['daily_change_op_supply'] = df['op_circulating_supply'].diff().fillna(0)
# For the first day, the difference is the 'op_circulating_supply' itself since it starts from 0
df['daily_change_op_supply'].iloc[0] = df['op_circulating_supply'].iloc[0]
#print
print(df[df['daily_change_op_supply']>0])
print(df.tail(5))


# In[ ]:


# Upload to Dune
du.write_dune_api_from_pandas(df, 'daily_op_circulating_supply',\
                             'Daily Snapshots of OP Token Circulating Supply, pulled from: https://static.optimism.io/tokenomics/circulatingSupply.txt')


# In[ ]:


bqu.write_df_to_bq_table(df, 'daily_op_circulating_supply')

