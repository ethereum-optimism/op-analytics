import requests as r
import pandas as pd

api_string = 'https://api.l2beat.com/api/'
# https://api.l2beat.com/api/tvl
# https://api.l2beat.com/api/activity
# https://l2beat.com/api/tvl/scaling.json
# https://l2beat.com/api/tvl/optimism.json

def get_l2beat_activity_data(data='activity',granularity='daily'):
        df = pd.DataFrame()

        api_url = api_string + data
        response = r.get(api_url)
        response.raise_for_status()  # Check if the request was successful
        json_data = response.json()['projects']
        
        # Create an empty list to collect rows
        rows_list = []
        
        # Iterate over the chains
        for chain_name in json_data:
        # if chain_name != 'combined':
                if data == 'activity':
                        daily_data = json_data[chain_name][granularity]['data']
                        types = json_data[chain_name][granularity]['types']
                elif data == 'tvl':
                        daily_data = json_data[chain_name]['charts'][granularity]['data']
                        types = json_data[chain_name]['charts'][granularity]['types']
                else:
                        print('not configured - need to configure for this API endpoint')
                        return

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

def get_all_l2beat_data(granularity='daily'):
        activity_df = get_l2beat_activity_data('activity',granularity)
        tvl_df = get_l2beat_activity_data('tvl',granularity)

        combined_df = tvl_df.merge(activity_df, on=['timestamp','chain'],how='outer')

        return combined_df