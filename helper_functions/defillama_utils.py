# NOTE: A lot of tech debt / legacy naming here. Things may not make sense. To be cleaned up!

import pandas as pd
import asyncio, aiohttp, nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
import requests as r
import numpy as np
import json
import re
from collections import defaultdict
import time
from datetime import datetime, date
import google_bq_utils as bqu
nest_asyncio.apply()


header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}
statuses = {x for x in range(100, 600)}
statuses.remove(200)
statuses.remove(429)

# Create a list of patterns to match
patterns_to_filter = [
    "-borrowed", "-staking", "-pool2", "-treasury",
    "^treasury$", "^borrowed$", "^staking$", "^pool2$"
]

# Create a function to check if a string matches any of the patterns
def matches_pattern(s):
    return any(re.search(pattern, s, re.IGNORECASE) for pattern in patterns_to_filter)

def has_overlap(a, b):
	return not set(a).isdisjoint(b)

def mod_dfl_dates(df, start_date='', date_column='date'):
	# Convert date column to datetime
	df[date_column] = pd.to_datetime(df[date_column], unit='s')
    
    # Filter rows
	if start_date == '':
		return df_filtered
	else:
		df_filtered = df[df[date_column] >= pd.to_datetime(start_date)]
		return df_filtered

async def get_tvl(session, apistring, chains, prot, prot_name, header = header, statuses = statuses, do_aggregate = 'No', fallback_on_raw_tvl = False, fallback_indicator = '*', start_date='2000-01-01'):#write_bq_dataset = '',write_bq_table=''):
		prod = []
		retry_client = RetryClient(client_session=session)  # Use the passed session

		async with retry_client.get(apistring, retry_options=ExponentialRetry(attempts=10), raise_for_status=statuses) as response:
				try:
						prot_req = await response.json()
						try: #error handling
								cats = prot_req['category']
						except:
								cats = ''
						try: # if parent protocol exists
								parent_prot_name = prot_req['parentProtocol']
						except: # if not, then use the name
								parent_prot_name = prot_name 
						prot_req = prot_req['chainTvls']
						prot_chains = list(prot_req.keys())

						if chains == '':
								chain_list = chains
						else:
								chain_list = [key for key in prot_chains if key in chains]

						for ch in chain_list:
								ad = pd.json_normalize( prot_req[ch]['tokens'] )
								ad_usd = pd.json_normalize( prot_req[ch]['tokensInUsd'] )
								ad = mod_dfl_dates(ad, start_date = start_date)
								ad_usd = mod_dfl_dates(ad_usd, start_date = start_date)


								if (ad_usd.empty) & (fallback_on_raw_tvl == True):
										ad = pd.DataFrame( prot_req[ch]['tvl'] )
										prot_map = prot + str(fallback_indicator)
								else:
										prot_map = prot
								try: #if there's generic tvl
										ad_tvl = pd.json_normalize( prot_req[ch]['tvl'] )
										ad_tvl = mod_dfl_dates(ad_tvl, start_date = start_date)
										ad_tvl = ad_tvl[['date','totalLiquidityUSD']]
										ad_tvl = ad_tvl.rename(columns={'totalLiquidityUSD':'total_app_tvl'})
										# Sort the DataFrame by date in descending order
										# ad_tvl['date'] = pd.to_datetime(ad_tvl['date'], unit='s')
										ad_tvl = ad_tvl.sort_values('date', ascending=False)
										# # Get the most recent total_app_tvl
										latest_tvl = ad_tvl.iloc[0]['total_app_tvl']
										ad_tvl['latest_total_app_tvl'] = latest_tvl
								except:
										continue
									 # ad = ad.merge(how='left')
								if not ad.empty:
										ad = pd.melt(ad,id_vars = ['date'])
										ad = ad.rename(columns={'variable':'token','value':'token_value'})


										if not ad_usd.empty:
												ad_usd = pd.melt(ad_usd,id_vars = ['date'])
												ad_usd = ad_usd.rename(columns={'variable':'token','value':'usd_value'})
												ad = ad.merge(ad_usd,on=['date','token'])
										
										else:
												ad['usd_value'] = ''
									
										if not ad_tvl.empty:
												ad = ad.merge(ad_tvl,on=['date'],how = 'outer')
										else:
												ad['total_app_tvl'] = ''
										
										try:
												ad['token'] = ad['token'].str.replace('tokens.','', regex=False)
										except:
												continue
										# if we have no token breakdown, take normal TVL 
										ad['usd_value'] = np.where(ad['token'] == 'totalLiquidityUSD', ad['total_app_tvl'], ad['usd_value'])
										#assign other cols
										ad['protocol'] = prot_map
										ad['slug'] = prot
										ad['chain'] = ch
										ad['category'] = cats
										ad['name'] = prot_name
										ad['parent_protocol'] = parent_prot_name

										if do_aggregate == 'Yes': #aggregate tokens
											ad = generate_flows_column(ad)
											ad = ad.groupby(['chain', 'date','protocol','parent_protocol']).agg(
												sum_token_value_usd_flow=pd.NamedAgg(column='token_value_usd_flow', aggfunc='sum'),
												sum_token_value_usd_price_change=pd.NamedAgg(column='token_value_usd_price_change', aggfunc='sum'),
												sum_usd_value=pd.NamedAgg(column='usd_value', aggfunc='sum')
											)
											ad = ad.reset_index()
										else: #maintain token-level
											ad['token'] = ad['token'].fillna('0').astype(str).infer_objects(copy=False)
											ad['token_value'] = ad['token_value'].fillna(0).astype('float64').infer_objects(copy=False)

										ad['to_filter_out'] = (ad['chain'].apply(matches_pattern) | 
																	(ad['protocol'] == "polygon-bridge-&-staking") | 
																	ad['protocol'].str.endswith("-cex")).astype(int)
										
								#		 ad['start_date'] = pd.to_datetime(prot[1])
										# ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
										# if write_bq_dataset != '':
										# 	bqu.append_and_upsert_df_to_bq_table(ad, table_id = write_bq_table, dataset_id= write_bq_dataset, unique_keys = ['date','chain','token','protocol'])
										# else:
										# 	prod.append(ad)
										
										prod.append(ad)
										ad = None #clear memory
										# print(ad)
				except Exception as e:
						raise Exception("Could not convert json")
				# finally:
				# 	await retry_client.close()
		
		# await retry_client.close()
		
		return prod

async def get_range(protocols, chains='', do_aggregate='No', fallback_on_raw_tvl=False, fallback_indicator='*', header=header, statuses=statuses, start_date='2000-01-01'):
    data_dfs = []
    fee_df = []
    if isinstance(chains, list):
        og_chains = chains
    elif chains == '':
        og_chains = chains
    else:
        og_chains = [chains]

    api_str = 'https://api.llama.fi/protocol/'
    prod = []
    tasks = []
    
    async with aiohttp.ClientSession() as session:  # Create a single session
        for index, proto in protocols.iterrows():
            prot = proto['slug']
            try:
                prot_name = proto['name']
            except:
                prot_name = ''
            try:
                if og_chains == '':
                    chains = proto['chainTvls']
                else:
                    chains = og_chains
            except:
                chains = og_chains
            apic = api_str + prot
            tasks.append(get_tvl(session, apic, chains, prot, prot_name, do_aggregate=do_aggregate, fallback_on_raw_tvl=fallback_on_raw_tvl, fallback_indicator=fallback_indicator, header=header, statuses=statuses, start_date=start_date))

        data_dfs = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results (this part remains unchanged)
    df_list = []
    for dat in data_dfs:
        if isinstance(dat, list):
            for pt in dat:
                try:
                    tempdf = pd.DataFrame(pt)
                    if not tempdf.empty:
                        df_list.append(tempdf)
                except:
                    continue
    df_df_all = pd.concat(df_list)
    df_df_all = df_df_all.fillna(0)
    
    data_dfs = []  # Free up Memory
    
    return df_df_all

def remove_bad_cats(netdf):
		summary_df = netdf[\
						(~netdf['chain'].str.contains('-borrowed')) &\
						(~netdf['chain'].str.contains('-staking')) &\
						(~netdf['chain'].str.contains('-pool2')) &\
						(~netdf['chain'].str.contains('-treasury')) &\
						(~netdf['chain'].str.contains('-vesting')) &\
						(~netdf['chain'].str.contains('-Vesting')) &\
						(~( netdf['chain'] == 'treasury') ) &\
						(~( netdf['chain'] == 'borrowed') ) &\
						(~( netdf['chain'] == 'staking') ) &\
						(~( netdf['chain'] == 'vesting') ) &\
						(~( netdf['chain'] == 'Vesting') ) &\
						(~( netdf['chain'] == 'pool2') )	   
#						 & (~( netdf_df['chain'] == 'Ethereum') )
						]
		return summary_df

def get_chain_tvls(chain_list):
		# get chain tvls
		chain_api = 'https://api.llama.fi/charts/'
		cl = []
		for ch in chain_list:
				try:
						capi = chain_api + ch
						cres = pd.DataFrame( r.get(capi, headers=header).json() )
						cres['chain'] = ch
						cres['date'] = pd.to_datetime(cres['date'], unit ='s') #convert to days
						cl.append(cres)
				except:
						continue
		chains = pd.concat(cl)
		return chains

def get_tvl_by_app_for_all_chains(protocol):
        def fetch_protocol_data(protocol_name):
                url = f"https://api.llama.fi/protocol/{protocol_name}"
                print(url)
                response = r.get(url)
                if response.status_code == 200:
                        return response.json()
                else:
                        print(f"Failed to fetch data for {protocol_name}. Status code: {response.status_code}")
                        return None

        def process_protocol_data(data):
                if not data:
                        return []

                result = []
                protocol = data['name']
                chains = data.get('chains', [])
                today = date.today()
                today_start_timestamp = int(datetime(today.year, today.month, today.day).timestamp())

                for chain in chains:
                        chain_data = data.get('chainTvls', {}).get(chain, {})
                        tvl_data = chain_data.get('tvl', [])
                        tokens_in_usd = chain_data.get('tokensInUsd', [])
                        tokens = chain_data.get('tokens', [])

                        # Filter for today's data
                        today_tvl = next((item for item in tvl_data if item['date'] >= today_start_timestamp), None)
                        today_tokens_usd = next((item for item in tokens_in_usd if item['date'] >= today_start_timestamp), None)
                        today_tokens = next((item for item in tokens if item['date'] >= today_start_timestamp), None)

                        if today_tvl and (today_tokens_usd or today_tokens):
                                tokens_usd = today_tokens_usd['tokens'] if today_tokens_usd else {}
                                tokens_value = today_tokens['tokens'] if today_tokens else {}

                                for token in set(list(tokens_usd.keys()) + list(tokens_value.keys())):
                                        result.append({
                                                'protocol': protocol,
                                                'chain': chain,
                                                'token': token,
                                                'USD value': tokens_usd.get(token, None),
                                                'token value': tokens_value.get(token, None)
                                        })

                return result

        # Fetch and process data for the given protocol
        data = fetch_protocol_data(protocol)
        if data:
                processed_data = process_protocol_data(data)
                df = pd.DataFrame(processed_data)
                return df
        else:
                return pd.DataFrame()  # Return an empty DataFrame if data fetch fails
    
# Eventually figure out how to integrate this with get_tvls so that it's not duplicative
def get_single_tvl(prot, chains, header = header, statuses = statuses, fallback_on_raw_tvl = False, print_api_str = False):
		prod = []
		# retry_client = RetryClient()
		apistring = 'https://api.llama.fi/protocol/' + prot
		if print_api_str:
				print(apistring)
		# response = retry_client.get(apistring, retry_options=ExponentialRetry(attempts=10), raise_for_status=statuses)
		try:
				prot_req = r.get(apistring).json()
				try:
						cats = prot_req['category']
				except:
						cats = ''
				prot_req = prot_req['chainTvls']
				for ch in chains:
						ch = ch.capitalize() # defillama uses initcap
						ad = pd.json_normalize( prot_req[ch]['tokens'] )
						ad_usd = pd.json_normalize( prot_req[ch]['tokensInUsd'] )
						if (ad_usd.empty) & (fallback_on_raw_tvl == True):
								ad = pd.DataFrame( prot_req[ch]['tvl'] )
						try: #if there's generic tvl
								ad_tvl = pd.json_normalize( prot_req[ch]['tvl'] )
								ad_tvl = ad_tvl[['date','totalLiquidityUSD']]
								ad_tvl = ad_tvl.rename(columns={'totalLiquidityUSD':'total_app_tvl'})
								# print(ad_tvl)
						except:
								continue
				#			 ad = ad.merge(how='left')
						if not ad.empty:
								ad = pd.melt(ad,id_vars = ['date'])
								ad = ad.rename(columns={'variable':'token','value':'token_value'})
								if not ad_usd.empty:
										ad_usd = pd.melt(ad_usd,id_vars = ['date'])
										ad_usd = ad_usd.rename(columns={'variable':'token','value':'usd_value'})
										ad = ad.merge(ad_usd,on=['date','token'])
								else:
										ad['usd_value'] = ''
								if not ad_tvl.empty:
										ad = ad.merge(ad_tvl,on=['date'],how = 'outer')
								else:
										ad['total_app_tvl'] = ''
								
								ad['date'] = pd.to_datetime(ad['date'], unit ='s') #convert to days
								try:
										ad['token'] = ad['token'].str.replace('tokens.','', regex=False)
								except:
										continue
								# if we have no token breakdown, take normal TVL 
								ad['usd_value'] = np.where(ad['token'] == 'totalLiquidityUSD', ad['total_app_tvl'], ad['usd_value'])
								#assign other cols
								ad['protocol'] = prot
								ad['chain'] = ch
								ad['category'] = cats
						#		 ad['start_date'] = pd.to_datetime(prot[1])
								# ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
								prod.append(ad)
								# print(ad)
		except json.JSONDecodeError as e:
				raise Exception(f"Error decoding JSON for protocol {prot}: {e}")
		except Exception as e:
				raise Exception(f"An unexpected error occurred for protocol {prot}: {e}")

		# retry_client.close()
		# print(prod)
		p_df = pd.concat(prod)
		return p_df

def get_protocols_by_chain(chain_name, exclude_cex = True, exclude_chain = True, return_cols = ''):
		protos = 'https://api.llama.fi/protocols'

		category_excludes = []
		if exclude_cex == True:
				category_excludes.append('CEX')
		if exclude_chain == True:
				category_excludes.append('Chain')

		s = r.Session()
		#get all protocols
		resp = pd.DataFrame( s.get(protos).json() )[['category','name','parentProtocol','slug','chainTvls']]
		resp['parentProtocol'] = resp['parentProtocol'].combine_first(resp['name'])
		# extract the chain names
		resp['chainTvls'] = resp['chainTvls'].apply(lambda x: list(x.keys()) )
		# set a true/false if the array contains the chain we want
		resp['contains_chain'] = resp['chainTvls'].apply(lambda x: chain_name in x)
		# set a true/false if the array doesn't contains the categories we want to exclude
		resp['contains_cats'] = resp['category'].apply(lambda x: x not in category_excludes)
		# filter where we have a match on chain
		proto_list = resp[resp['contains_chain'] == True]
		# filter where we have a match on cats
		proto_list = proto_list[proto_list['contains_cats'] == True]
		# clean up
		if return_cols != '':
				# proto_list = proto_list[['slug']]
				proto_list = proto_list[return_cols]
		proto_list = proto_list.reset_index(drop=True)
		# boom
		return proto_list

def get_protocol_names_by_flag(check_flag):
		flag_str = '-' + check_flag
		protocols = r.get('https://api.llama.fi/lite/protocols2', headers=header).json()
		print(protocols)
		protocols = protocols['protocols']
		protocols = [protocol for protocol in protocols if any(flag_str in key for key in protocol['chainTvls'])]
		protocol_names = [element['name'] for element in protocols]
		return protocol_names

def get_protocol_tvls(min_tvl = 0, excluded_cats = ['CEX','Chain'], chains = ''): #,excluded_flags = ['staking','pool2']):
		all_api = 'https://api.llama.fi/protocols'
		resp = pd.DataFrame( r.get(all_api, headers=header).json() )
		resp = resp[resp['tvl'] > min_tvl ] ##greater than X
		if excluded_cats != []: #If we have cagtegories to exclude
				resp = resp[~resp['category'].isin(excluded_cats)]
		
		# Check Chain List
		if isinstance(chains, list):
				og_chains = chains #get starting value
		elif chains == '':
				og_chains = chains
				return resp # Return if chain list is null
		else:
				og_chains = [chains] #make it a list
		resp = resp[resp['chains'].apply(lambda x: has_overlap(x, og_chains))]

		return resp

async def async_get_all_protocol_tvls_by_chain_and_token(min_tvl, chains, do_aggregate, fallback_on_raw_tvl, fallback_indicator, excluded_cats, start_date):
    res = get_protocol_tvls(min_tvl, excluded_cats=excluded_cats, chains=chains)
    print(f'Number of Apps: {len(res)}')
    protocols = res[['slug', 'name', 'category', 'parentProtocol', 'chainTvls']]
    res = []  # Free up memory

    protocols['parentProtocol'] = protocols['parentProtocol'].combine_first(protocols['name'])
    protocols['chainTvls'] = protocols['chainTvls'].apply(lambda x: list(x.keys()))
    
    # Call get_range asynchronously
    df_df = await get_range(protocols, chains, do_aggregate=do_aggregate, fallback_on_raw_tvl=fallback_on_raw_tvl, fallback_indicator=fallback_indicator, start_date=start_date)
    
    protocols = []  # Free up memory

    print(f'Number of Rows: {len(df_df)}')

    return df_df

def get_all_protocol_tvls_by_chain_and_token(min_tvl=0, chains='', do_aggregate='No', fallback_on_raw_tvl=False, fallback_indicator='*', excluded_cats=['CEX', 'Chain'], start_date='2000-01-01'):
    return asyncio.run(async_get_all_protocol_tvls_by_chain_and_token(
        min_tvl, chains, do_aggregate, fallback_on_raw_tvl, fallback_indicator, excluded_cats, start_date
    ))

def get_latest_defillama_prices(token_list, chain = 'optimism'):

		token_list = ','.join([f"{chain}:{token}" for token in token_list])

		llama_api = 'https://coins.llama.fi/prices/current/' + token_list + '?searchWidth=4h'
		prices = pd.DataFrame(r.get(llama_api,headers=header).json()['coins']).T.reset_index()

		prices = prices.rename(columns={'index':'token_address'})

		prices[['chain', 'token_address']] = prices['token_address'].str.split(':', expand=True)

		return prices

def get_historical_defillama_prices(token_list_api, chain = 'optimism', min_ts = 0):
		
		token_list_api = ','.join([f"{chain}:{token}" for token in token_list_api])

		llama_api = 'https://coins.llama.fi/chart/' + token_list_api \
						+ '?start=' + str(min_ts) \
						+ '&span=600&period=1d&searchWidth=300'
		# print(llama_api)
		try:
				# prices = pd.DataFrame(r.get(llama_api,headers=header).json()['coins']).T.reset_index()
				prices = r.get(llama_api,headers=header).json()
				prices = pd.DataFrame(prices['coins']).T
				prices.reset_index(inplace=True)
				prices = prices.rename(columns={'index':'token_address'})
				prices = prices.loc[:, ['token_address', 'symbol', 'decimals', 'prices']]
				
				result = pd.DataFrame()
				for i, prices_ in enumerate(prices['prices']):
						data = [{'timestamp': x['timestamp'], 'price': x['price']} for x in prices_]
						new_df = pd.concat([prices.iloc[i, :].drop(columns=['prices']).to_frame().T.assign(**price) for price in data], axis=0, ignore_index=True)
						result = pd.concat([result, new_df], axis=0, ignore_index=True)

				result.drop(columns=['prices'], inplace=True)

				result['date'] = pd.to_datetime(result['timestamp'], unit='s').dt.date

				result[['chain', 'token_address']] = result['token_address'].str.split(':', expand=True)

		except:
				result = pd.DataFrame(columns=['token_address', 'symbol', 'decimals','timestamp',\
												'price','date','chain'])

		return result

def get_todays_tvl():
		api_string =' https://api.llama.fi/protocols'
		tvltoday = r.get(api_string,headers=header).json()

		# Extract relevant data from JSON
		df_data = []
		meta_cols = ['name', 'parentProtocol', 'category','slug','url','description','twitter','forkedFrom','oracles','tvl']
		for entry in tvltoday:
				arr = []
				for col in meta_cols:
						try:
								value = entry[col]
						except:
								value = ''
								
						arr.append(value)


				chain_tvls = entry['chainTvls']
				for chain, chain_tvl in chain_tvls.items():
						df_data.append(arr + [chain, chain_tvl])

		exp_cols = meta_cols  + ['chain','chainTVL']

		df = pd.DataFrame(df_data, columns= exp_cols)
		
		df['parent_slug'] = df['parentProtocol'].str.replace("parent#", "")
		df['parent_name'] = df['parent_slug'].str.replace("_", " ").str.title()

		# Drop helper columns
		columns_to_drop = ['parent_slug', 'parentProtocol']
		df = df.drop(columns_to_drop, axis=1)
		
		return df

def get_historical_chain_tvl(chain_name):
		df = pd.DataFrame()
		api_string = 'https://api.llama.fi/v2/historicalChainTvl/' + chain_name
		try:
				tvl = r.get(api_string,headers=header).json()
				df = pd.DataFrame(tvl)
				df['date'] = pd.to_datetime(df['date'], unit='s')
		except:
				print('error - historicalChainTvl api')
		return df


def get_historical_app_tvl_by_chain(chain_name):
		p = get_protocol_tvls()

def generate_flows_column(df):
		# Still need to debug the price flow
		try: 
				df['token_value'] = df['token_value'].replace(0, np.nan) #Never divide by 0
				df['price_usd'] = df['usd_value'] / df['token_value']
				df['token_value'] = df['token_value'].fillna(0) #Fill back 0s
				
				# Sort the DataFrame by date
				df.sort_values(by='date', ascending=True, inplace=True)

				# Get Prior Values to Diff
				df['prior_price_usd'] = df.groupby(['token', 'protocol', 'chain'])['price_usd'].shift(1)
				df['prior_token_value'] = df.groupby(['token', 'protocol', 'chain'])['token_value'].shift(1)

				# Back/Front Fill values in case no diff
				df['prior_price_usd'] = (df[['prior_price_usd', 'price_usd']].bfill(axis=1).iloc[:, 0])
				df['prior_token_value'] = (df[['prior_token_value', 'token_value']].bfill(axis=1).iloc[:, 0])
				
				# Sort, so that "last" is the most recent date, since this skips nan, but first doesn't.
				df.sort_values(by='date', ascending=False, inplace=True)
				df['latest_price_usd'] = df.groupby(['token', 'protocol', 'chain'])['price_usd'].transform('last')
				
				#Resort
				df.sort_values(by='date', ascending=True, inplace=True)

				# Fill Gaps
				df = df.fillna(0).infer_objects(copy=False)
				# Get Differences
				df['price_usd_change'] = df['price_usd'] - df['prior_price_usd']
				df['token_value_change'] = df['token_value'] - df['prior_token_value']

				# Calculate Token Value Flow USD and Token Value Price Change USD
				df['token_value_usd_flow'] = df['token_value_change'] * df['price_usd']
				df['token_value_usd_price_change'] = df['prior_token_value'] * df['price_usd_change']

				# Apply the override: If token value goes to 0, make it all token value flow and 0 price change flow
				mask = df['token_value'] == 0
				df.loc[mask, 'token_value_usd_flow'] = -df.loc[mask, 'prior_token_value'] * df.loc[mask, 'prior_price_usd']
				df.loc[mask, 'token_value_usd_price_change'] = 0
				
				# Drop the unnecessary columns
				df.drop(['prior_price_usd', 'prior_token_value'], axis=1, inplace=True)

		except KeyError as e:
				print(f"Error: {e} column not found.")

		return df


def get_chain_category_data():

	# Step 1: Download the TypeScript file content
	url = "https://raw.githubusercontent.com/DefiLlama/defillama-server/master/defi/src/adaptors/data/helpers/chains/index.ts"
	response = r.get(url)
	ts_content = response.text

	# Step 2: Extract JSON-like part from the TypeScript content
	start_index = ts_content.find("export default {") + len("export default ")
	end_index = ts_content.find("} as unknown as") + 1
	json_like_content = ts_content[start_index:end_index].strip()

	# Step 3: Clean and convert JSON-like string to a proper JSON string
	# Add quotes around keys
	json_like_content = re.sub(r'(\w+):', r'"\1":', json_like_content)

	# Step 4: Remove comments, except for URLs
	# json_like_content = re.sub(r'(?<!http:)(?<!https:)//.*', '', json_like_content)
	json_like_content = re.sub(r'(?<!:)//(?!/?/).*$', '', json_like_content, flags=re.MULTILINE)

	# Step 5: Replace single quotes with double quotes
	json_like_content = json_like_content.replace("'", '"')

	# Step 6: Replace `None` with `null`
	json_like_content = json_like_content.replace("None", "null")

	# Step 7: Remove trailing commas before closing braces
	json_like_content = re.sub(r',\s*([}\]])', r'\1', json_like_content)
	json_like_content = json_like_content.replace('""https":', '"https:')


	# Step 8: Ensure the content is wrapped in curly braces
	json_like_content = f"{{{json_like_content.strip()[1:-1]}}}"

	# Step 9: Convert JSON-like string to a Python dictionary
	try:
		data_dict = json.loads(json_like_content)
	except json.JSONDecodeError as e:
		print(f"Error decoding JSON: {e}")
		print(json_like_content)
		raise

	# Step 10: Prepare data for the DataFrame
	data = defaultdict(list)

	for chain, attributes in data_dict.items():
		data['chain'].append(chain)
		data['geckoId'].append(attributes.get('geckoId'))
		data['symbol'].append(attributes.get('symbol'))
		data['cmcId'].append(attributes.get('cmcId'))
		data['categories'].append(attributes.get('categories'))
		data['chainId'].append(attributes.get('chainId'))
		parent = attributes.get('parent', {})
		data['parent_chain'].append(parent.get('chain'))
		data['parent_types'].append(parent.get('types'))

	# Step 11: Create DataFrame
	df = pd.DataFrame(data)

	df['chain_id'] = df['chain_id'].astype(str).str.replace('.0','')

	return df

def get_config():
	# Fetch JSON data from the API
	url = "https://api.llama.fi/config"
	response = r.get(url)
	data = response.json()
	return data

def get_protocols_config():
	data = get_config()
	protocols_data = data["protocols"]

	# Create pandas dataframe for protocols
	protocols_df = pd.DataFrame(protocols_data)
	return protocols_df

def get_chains_config():
	data = get_config()
	chains_data = data["chainCoingeckoIds"]
	
	# Create pandas dataframe for chains
	chains_df = pd.DataFrame(chains_data)
	# Transpose the chains dataframe
	chains_df = chains_df.transpose()
	# Combine the two "chainId" fields into one
	chains_df.fillna({'chainId': chains_df['chainid']}, inplace=True)
	chains_df.drop(columns=['chainid'], inplace=True)
	# Reset index and rename index column
	chains_df.reset_index(inplace=True)
	chains_df.rename(columns={'index': 'defillama_slug'}, inplace=True)

	return chains_df


def get_fees_revenue(chain_name, exclude_total_data_chart=True, exclude_total_data_chart_breakdown=True, data_type='totalRevenue'):
    # Validate data_type
    valid_data_types = ['totalFees', 'totalRevenue', 'dailyFees', 'dailyRevenue']
    if data_type not in valid_data_types:
        raise ValueError(f"Invalid data_type. Must be one of {valid_data_types}")

    # Construct the API URL
    base_url = "https://api.llama.fi/overview/fees"
    url = f"{base_url}/{chain_name}"

    # Set up query parameters
    params = {
        'excludeTotalDataChart': str(exclude_total_data_chart).lower(),
        'excludeTotalDataChartBreakdown': str(exclude_total_data_chart_breakdown).lower(),
        'dataType': data_type
    }

    # Make the API request
    response = r.get(url, params=params)
    response.raise_for_status()  # Raise an exception for bad responses

    # Parse the JSON response
    data = response.json()

    # Extract the relevant data and create a DataFrame
    protocols = data.get('protocols', [])
    df = pd.DataFrame(protocols)

    return df
