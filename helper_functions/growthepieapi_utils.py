from functools import reduce
import pandas as pd
import requests
import numpy as np

url_base = 'https://api.growthepie.xyz/'

def get_growthepie_api_data():
    ###
    # Tx Data
    ###
    data_url = url_base + 'v1/fundamentals_full.json'
    # Make an HTTP GET request to the URL
    response = requests.get(data_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Load JSON data from the response
        data_json = response.json()
        
        # Convert JSON data to pandas DataFrame
        df = pd.DataFrame(data_json).reset_index()
        
        # Pivot table to get 'metric_key' and 'value' combination as columns
        pivot_df = df.pivot_table(index=["origin_key", "date"], columns="metric_key", values="value").reset_index()

        pivot_df = pivot_df.replace(0, np.nan)
        # Drop rows where all metric columns (excluding 'origin_key' and 'date') are NaN
        filtered_data_df = pivot_df.dropna(subset=[col for col in pivot_df.columns if col not in ['origin_key', 'date']], how='all')
        filtered_data_df = filtered_data_df.reset_index()
        return filtered_data_df
    else:
        print("Failed to retrieve data. HTTP Status Code:", response.status_code)

def get_growthepie_api_meta():
    ###
    # MetaData
    ###
    meta_url = url_base + 'v1/master.json'
    response = requests.get(meta_url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Access the 'chains' part of the data
        chains_data = data['chains']
        
        # Prepare an empty list to store the normalized data
        normalized_data = []
        
        # Iterate over each chain in the 'chains' data
        for chain_meta, attributes in chains_data.items():
            # Add the 'chain_meta' as part of the attributes
            attributes['origin_key'] = chain_meta
            # Append the attributes (including 'chain_meta') to the list
            normalized_data.append(attributes)
        
        # Convert the list of dictionaries into a DataFrame
        df_chains = pd.DataFrame(normalized_data).reset_index(drop=True)
        df_chains = df_chains.rename(columns={'name_short':'chain_name'})
        return df_chains
    else:
        print("Failed to retrieve data. HTTP Status Code:", response.status_code)

#######
def get_growthepie_api_data_legacy():
    base_url = "https://api.growthepie.xyz/"
    endpoints = [
        "v1/metrics/daa.json",
        "v1/metrics/txcount.json",
        "v1/metrics/txcosts.json",
        "v1/metrics/tvl.json",
        "v1/metrics/fees.json",
        "v1/metrics/stables.json",
    ]
    
    dataframes = []

    for endpoint in endpoints:
        metric_name = endpoint.split('/')[-1].replace('.json', '')
        
        # Get data from the API
        response = requests.get(base_url + endpoint)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Create a list to store the rows
            rows = []
            
            # Loop through the data and create a row for each date and chain
            for raw_chain_data in data['data']['chains'].items():
                chain_origin_key = raw_chain_data[0]
                chain_data = raw_chain_data[1]
                chain_name = chain_data['chain_name']
                types = chain_data['daily']['types'][1:]
                for daily_data in chain_data['daily']['data']:
                    unix_timestamp, *values = daily_data
                    date = pd.to_datetime(unix_timestamp, unit='ms')
                    row = {'chain_origin_key': chain_origin_key, 'chain_name': chain_name, 'date': date}
                    for i, value in enumerate(values):
                        suffix = f"_{types[i]}" if len(values) > 1 else ''
                        row[f"{metric_name}{suffix}"] = value
                    rows.append(row)
            
            # Create a DataFrame from the rows
            df = pd.DataFrame(rows)
            
            # Set the 'date' and 'chain_name' columns as the index
            df.set_index(['date', 'chain_name','chain_origin_key'], inplace=True)
            
            dataframes.append(df)
            # print(metric_name)
            # print(df.reset_index()['chain_origin_key'].drop_duplicates())
        else:
            print(f"Failed to retrieve data for {metric_name}. HTTP Status code: {response.status_code}")

    # Merge all dataframes into a single dataframe
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'chain_name','chain_origin_key'], how='outer'), dataframes)
    merged_df = merged_df.reset_index()
    
    return merged_df