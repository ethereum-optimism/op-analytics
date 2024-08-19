import pandas as pd
import requests as r
import time

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}

def get_daily_token_data(cg_id, show_current=True):
        final_df = get_token_data_by_granularity(cg_id, 'daily', show_current)

        return final_df

def get_token_data_by_granularity(cg_id, granularity='daily', show_current=True, num_days=365, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            info_api = 'https://api.coingecko.com/api/v3/coins/' + cg_id
            info = r.get(info_api, headers=header).json()
            symbol = info['symbol']
            cg_name = info['name']
            details = info['detail_platforms']

            api_base = f'https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart?vs_currency=usd&days={num_days}&interval={granularity}'
            print(api_base)
            res = r.get(api_base, headers=header)
            
            if res.status_code == 429:  # Rate limit error
                print(f"Rate limited. Attempt {attempt + 1} of {max_retries}. Waiting {delay} seconds...")
                time.sleep(delay)
                continue
            
            res.raise_for_status()  # Raise an exception for other HTTP errors
            res = res.json()

            prices = pd.DataFrame(res['prices'], columns=['date', 'prices'])
            mktcaps = pd.DataFrame(res['market_caps'], columns=['date', 'market_caps'])
            volumes = pd.DataFrame(res['total_volumes'], columns=['date', 'total_volumes'])
            arr_list = [prices, mktcaps, volumes]
            final_df = pd.DataFrame()
            for arr in arr_list:
                arr['date'] = pd.to_datetime(arr['date'], unit='ms')
                if final_df.empty:
                    final_df = arr
                else:
                    final_df = final_df.merge(arr, on='date', how='outer')
            
            if not show_current:
                final_df.drop(final_df.tail(1).index, inplace=True)  # drop last 1 row

            final_df['cg_id'] = cg_id
            final_df['symbol'] = symbol
            final_df['name'] = cg_name
            final_df['details'] = str(details)

            return final_df.sort_values(by='date', ascending=True)

        except r.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Unable to fetch data.")
                return pd.DataFrame()  # Return an empty DataFrame if all retries fail

    return pd.DataFrame()  # Return an empty DataFrame if the loop completes without success