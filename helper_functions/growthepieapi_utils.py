from functools import reduce
import pandas as pd
import requests

def get_growthepie_api_data():
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
            for chain, chain_data in data['data']['chains'].items():
                chain_name = chain_data['chain_name']
                types = chain_data['daily']['types'][1:]
                for daily_data in chain_data['daily']['data']:
                    unix_timestamp, *values = daily_data
                    date = pd.to_datetime(unix_timestamp, unit='ms')
                    row = {'chain_name': chain_name, 'date': date}
                    for i, value in enumerate(values):
                        suffix = f"_{types[i]}" if len(values) > 1 else ''
                        row[f"{metric_name}{suffix}"] = value
                    rows.append(row)
            
            # Create a DataFrame from the rows
            df = pd.DataFrame(rows)
            
            # Set the 'date' and 'chain_name' columns as the index
            df.set_index(['date', 'chain_name'], inplace=True)
            
            dataframes.append(df)
        else:
            print(f"Failed to retrieve data for {metric_name}. HTTP Status code: {response.status_code}")

    # Merge all dataframes into a single dataframe
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=['date', 'chain_name'], how='outer'), dataframes)
    merged_df = merged_df.reset_index()
    
    return merged_df