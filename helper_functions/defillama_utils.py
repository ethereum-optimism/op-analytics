# NOTE: A lot of tech debt / legacy naming here. Things may not make sense. To be cleaned up!

import pandas as pd
import asyncio, aiohttp, nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
import requests as r
import numpy as np
import time
nest_asyncio.apply()

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}
statuses = {x for x in range(100, 600)}
statuses.remove(200)
statuses.remove(429)

async def get_tvl(apistring, header, statuses, chains, prot, prot_name, fallback_on_raw_tvl = False, fallback_indicator = '*'):
        prod = []
        retry_client = RetryClient()

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
                        for ch in chains:
                                ad = pd.json_normalize( prot_req[ch]['tokens'] )
                                ad_usd = pd.json_normalize( prot_req[ch]['tokensInUsd'] )
                                if (ad_usd.empty) & (fallback_on_raw_tvl == True):
                                        ad = pd.DataFrame( prot_req[ch]['tvl'] )
                                        prot_map = prot + str(fallback_indicator)
                                else:
                                        prot_map = prot
                                try: #if there's generic tvl
                                        ad_tvl = pd.json_normalize( prot_req[ch]['tvl'] )
                                        ad_tvl = ad_tvl[['date','totalLiquidityUSD']]
                                        ad_tvl = ad_tvl.rename(columns={'totalLiquidityUSD':'total_app_tvl'})
                                        # print(ad_tvl)
                                except:
                                        continue
                        #             ad = ad.merge(how='left')
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
                                        ad['protocol'] = prot_map
                                        ad['slug'] = prot
                                        ad['chain'] = ch
                                        ad['category'] = cats
                                        ad['name'] = prot_name
                                        ad['parent_protocol'] = parent_prot_name
                                #         ad['start_date'] = pd.to_datetime(prot[1])
                                        # ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
                                        prod.append(ad)
                                        # print(ad)
                except Exception as e:
                        raise Exception("Could not convert json")
        await retry_client.close()
        # print(prod)
        return prod

def get_range(protocols, chains = '', fallback_on_raw_tvl = False, fallback_indicator = '*', header = header, statuses = statuses):
        data_dfs = []
        fee_df = []
        if isinstance(chains, list):
                og_chains = chains #get starting value
        elif chains == '':
                og_chains = chains
        else:
                og_chains = [chains] #make it a list
        # for dt in date_range:
        #         await asyncio.gather()
        #         data_dfs.append(res_df)
        #         # res.columns
        # try:
        #         loop.close()
        # except:
        #         #nothing
        loop = asyncio.get_event_loop()
        #get by app
        api_str = 'https://api.llama.fi/protocol/'
        # print(protocols)
        prod = []
        tasks = []
        for index,proto in protocols.iterrows():
                prot = proto['slug']
                ##
                try:
                        prot_name = proto['name']
                except:
                        prot_name = ''
                ##
                try:
                        if og_chains == '':
                                chains = proto['chainTvls']
                        else:
                                chains = og_chains
                except:
                        chains = og_chains
                apic = api_str + prot
                tasks.append( get_tvl(apic, header, statuses, chains, prot, prot_name, fallback_on_raw_tvl, fallback_indicator) )

        data_dfs = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

        df_list = []
        for dat in data_dfs:
                if isinstance(dat,list):
                        # print(dat)
                        for pt in dat: #each list within the list (i.e. multiple chains)
                                try:
                                        tempdf = pd.DataFrame(pt)
                                        if not tempdf.empty:
                                                # print(tempdf)
                                                df_list.append(tempdf)
                                except:
                                        continue
        df_df_all = pd.concat(df_list)
        df_df_all = df_df_all.fillna(0)
        
        data_dfs = [] #Free up Memory
        
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
#                         & (~( netdf_df['chain'] == 'Ethereum') )
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

# Eventually figure out how to integrate this with get_tvls so that it's not duplicative
def get_single_tvl(prot, chains, header = header, statuses = statuses, fallback_on_raw_tvl = False):
        prod = []
        # retry_client = RetryClient()
        apistring = 'https://api.llama.fi/protocol/' + prot
        # print(apistring)
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
                #             ad = ad.merge(how='left')
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
                        #         ad['start_date'] = pd.to_datetime(prot[1])
                                # ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
                                prod.append(ad)
                                # print(ad)
        except Exception as e:
                raise Exception("Could not convert json")
        # retry_client.close()
        # print(prod)
        p_df = pd.concat(prod)
        return p_df

def get_protocols_by_chain(chain_name, exclude_cex = True, exclude_chain = True):
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
        proto_list = proto_list[['slug']]
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


def get_protocol_tvls(min_tvl = 0, excluded_cats = ['CEX','Chain']): #,excluded_flags = ['staking','pool2']):
        all_api = 'https://api.llama.fi/protocols'
        resp = pd.DataFrame( r.get(all_api, headers=header).json() )
        resp = resp[resp['tvl'] > min_tvl ] ##greater than X
        if excluded_cats != []: #If we have cagtegories to exclude
                resp = resp[~resp['category'].isin(excluded_cats)]
        # Get Other Flags -- not working right now?
        # doublecounts = get_protocol_names_by_flag('doublecounted')
        # liqstakes = get_protocol_names_by_flag('liquidstaking')
        # resp = resp.assign(is_doubelcount = resp['name'].isin(doublecounts))
        # resp = resp.assign(is_liqstake = resp['name'].isin(liqstakes))
        # if excluded_flags != []: #If we have cagtegories to exclude
        #         for flg in excluded_flags:
        #                 resp = resp[resp[flg] != True]
        return resp

def get_all_protocol_tvls_by_chain_and_token(min_tvl = 0, fallback_on_raw_tvl = False, fallback_indicator='*', excluded_cats = ['CEX','Chain']):
        res = get_protocol_tvls(min_tvl)
        protocols = res[['slug','name','category','parentProtocol','chainTvls']]
        res = [] #Free up memory

        protocols['parentProtocol'] = protocols['parentProtocol'].combine_first(protocols['name'])
        protocols['chainTvls'] = protocols['chainTvls'].apply(lambda x: list(x.keys()) )
        df_df = get_range(protocols, '', fallback_on_raw_tvl, fallback_indicator)
        protocols = [] #Free up memory

        # Get Other Flags -- not working right now?
        # proto_info = res[['name','is_doubelcount','is_liqstake']]
        # df_df = df_df.merge(proto_info,on='name',how='left')

        return df_df

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
        meta_cols = ['name', 'category','slug','url','description','twitter','forkedFrom','oracles','tvl']
        for entry in tvltoday:
                arr = []
                for col in meta_cols:
                        try:
                                arr.append(entry[col])
                        except:
                                arr.append('')
                chain_tvls = entry['chainTvls']
                for chain, chain_tvl in chain_tvls.items():
                        df_data.append(arr + [chain, chain_tvl])

        # Create DataFrame
        # columns = ['Name', 'Category', 'Chain', 'Slug', 'TVL', 'ChainTVL']
        df = pd.DataFrame(df_data, columns= meta_cols + ['chain','chainTVL'])
        # Display the DataFrame
        return df