# Utility functions for pulling data from Growthepie (get it?) API
# Growthepie API Documentation: https://docs.growthepie.xyz/api

# OP Analytics
# See Legacy Content in: https://github.com/ethereum-optimism/op-analytics/tree/main/helper_functions
# See Newer Implementation in: https://github.com/ethereum-optimism/op-analytics/tree/main/src/op_analytics
#  - Documentation: https://static.optimism.io/op-analytics/sphinx/html/index.html

import pandas as pd
import requests as r
import numpy as np

url_base = 'https://api.growthepie.xyz/'

def get_growthepie_fundamentals():
    data_url = f"{url_base}v1/fundamentals_full.json"
    print(data_url)
    response = r.get(data_url)

    # Process response
    if response.status_code == 200:
        data_json = response.json()
        df = pd.DataFrame(data_json).reset_index()
        # Pivot
        pivot_df = df.pivot_table(index=["origin_key", "date"], columns="metric_key", values="value").reset_index()
        pivot_df = pivot_df.replace(0, np.nan)
        # Drop rows where all metric columns (excluding 'origin_key' and 'date') are NaN
        filtered_data_df = pivot_df.dropna(subset=[col for col in pivot_df.columns if col not in ['origin_key', 'date']], how='all')
        filtered_data_df = filtered_data_df.reset_index()

        filtered_data_df['date'] = pd.to_datetime(filtered_data_df['date'])

        return filtered_data_df
    else:
        print("Failed to retrieve data. HTTP Status Code:", response.status_code)

def get_growthepie_metadata():
    meta_url = f"{url_base}v1/master.json"
    response = r.get(meta_url)

    # Process response
    if response.status_code == 200:
        data = response.json()
        chains_data = data['chains']
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
