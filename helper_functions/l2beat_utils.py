import requests as r
import pandas as pd

api_string = 'https://api.l2beat.com/api/'
# https://api.l2beat.com/api/tvl
# https://api.l2beat.com/api/activity

def get_l2beat_activity_data():
        api_url = api_string + 'activity'
        response = r.get(api_url)
        response.raise_for_status()  # Check if the request was successful
        json_data = response.json()['projects']
        
        # Create an empty list to collect rows
        rows_list = []
        
        # Iterate over the chains
        for chain_name in json_data:
        # if chain_name != 'combined':
                daily_data = json_data[chain_name]['daily']['data']
                types = json_data[chain_name]['daily']['types']

                # Iterate through each day's data
                for day_data in daily_data:
                        # Create a dictionary for each day's data
                        data_dict = dict(zip(types, day_data))
                        data_dict['chain'] = chain_name  # Add the chain name to the dictionary
                        rows_list.append(data_dict)
        
        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows_list)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') 
        return df
