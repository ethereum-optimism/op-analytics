# Utility functions for pulling data from Defillama API & scraping GitHub
# Defillama API Documentation: https://defillama.com/docs/api

# OP Analytics
# See Legacy Content in: https://github.com/ethereum-optimism/op-analytics/tree/main/helper_functions
# See Newer Implementation in: https://github.com/ethereum-optimism/op-analytics/tree/main/src/op_analytics
#  - Documentation: https://static.optimism.io/op-analytics/sphinx/html/index.html

import pandas as pd
import requests as r
import api_utilities as ap

def get_latest_chain_tvl():
	session = ap.new_session()
	chains_api = 'https://api.llama.fi/v2/chains'
	
	chains_response = ap.get_data(session,chains_api)
	df = pd.DataFrame(chains_response)
	return df

def get_chain_list():
	
	df = get_latest_chain_tvl()
	df = df[['name','chainId']]
	df = df.drop_duplicates()
	
	return df

def get_historical_chain_tvl(chain_name):
    session = ap.new_session()
    api_string = f"https://api.llama.fi/v2/historicalChainTvl/{chain_name}"

    try:
        tvl_response = ap.get_data(session,api_string)
        df = pd.DataFrame(tvl_response)
        df['date'] = pd.to_datetime(df['date'], unit='s')

    except Exception as e:
        error_message = f"{chain_name} - error - historicalChainTvl api: {str(e)}"
        return None  # Return None instead of an empty DataFrame on error
    return df

def get_all_chains_historical_tvl(chain_list_df):
    num_projects = len(chain_list_df)
    print(f"Defillama API, Chains to run: {num_projects}")
    
    dfs = []
    i = 0

    for index, row in chain_list_df.iterrows():
        try:
            tvl_df = get_historical_chain_tvl(row['name'])
            i += 1

            if not tvl_df.empty:
                tvl_df['name'] = row['name']
                tvl_df['chain_id'] = row['chainId']
                dfs.append(tvl_df)
            else:
                print(f"Warning: Empty DataFrame returned for chain {row['name']}")
        except Exception as e:
            print(f"Error processing chain {row['name']}: {str(e)}")
        
        if i % 25 == 0:
            print(f"{i} / {num_projects} completed")
    
    if not dfs:
        print("Warning: No valid data was retrieved for any chain.")
        return pd.DataFrame()  # Return an empty DataFrame instead of raising an error
    
    
    all_dfs = pd.concat(dfs, ignore_index=True)
    return all_dfs
