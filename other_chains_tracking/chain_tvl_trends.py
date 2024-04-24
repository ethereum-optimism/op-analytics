#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start tvl trends')
import pandas as pd
import sys
import numpy as np
import json
from datetime import datetime, timedelta
sys.path.append("../helper_functions")
import defillama_utils as dfl
import duneapi_utils as du
import opstack_metadata_utils as ops
sys.path.pop()

current_utc_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
print(current_utc_date)


# In[ ]:


min_tvl_to_count_apps = 0


# In[ ]:


# OPStack Metadata will auto pull, but we can also add curated protocols here (we handle for dupes)
curated_chains = [
    #L2s
     ['Optimism', 'L2']
    ,['Base', 'L2']
    ,['Arbitrum', 'L2']
    #non-dune L2s
    ,['ZkSync Era', 'L2']
    ,['Polygon zkEVM', 'L2']
    ,['Starknet', 'L2']
    ,['Linea', 'L2']
    ,['Mantle', 'L2']
    ,['Scroll', 'L2']
    ,['Boba', 'L2']
    ,['Metis', 'L2']
    ,['opBNB', 'L2']
    ,['Rollux', 'L2']
    ,['Manta', 'L2']
    ,['Kroma','L2']
    ,['Arbitrum%20Nova','L2']
    #L1
    ,['Ethereum', 'L1']
    #Others
    ,['Fantom', 'L1']
    ,['Avalanche', 'L1']
    ,['Gnosis' , 'L1']
    ,['Celo', 'L1']
    ,['Polygon', 'L1']
    ,['BSC', 'L1']
]

curated_protocols = [
         ['aevo', 'L2',['optimism','arbitrum','ethereum']]
        ,['dydx', 'L2',['ethereum']]
        ,['immutablex', 'L2',['ethereum']]
        ,['apex-protocol', 'L2',['ethereum']]
        # ,['brine.fi', 'L2',['ethereum']] #no longer listed
        ,['loopring', 'L2',['ethereum']]
        ,['aztec', 'L2',['ethereum']]
        ,['lyra-v2', 'L2', ['ethereum']]
    ]


# In[ ]:


opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')
#filter to defillama chains
tvl_opstack_metadata = opstack_metadata[~opstack_metadata['defillama_slug'].isna()].copy()
# opstack_chains
tvl_opstack_metadata = tvl_opstack_metadata[['defillama_slug','chain_layer']]

opstack_chains = tvl_opstack_metadata[~tvl_opstack_metadata['defillama_slug'].str.contains('protocols/')].copy()
opstack_protocols = tvl_opstack_metadata[tvl_opstack_metadata['defillama_slug'].str.contains('protocols/')].copy()
#clean column
opstack_protocols['defillama_slug'] = opstack_protocols['defillama_slug'].str.replace('protocols/','')
# Use apply to create an array with one element
opstack_protocols['chain_list'] = opstack_protocols.apply(lambda x: ['ethereum'], axis=1) #guess, we can manually override if not


# In[ ]:


# print(opstack_chains)


# In[ ]:


# Create new lists
chains = curated_chains.copy()
protocols = curated_protocols.copy()
# Iterate through the DataFrame and append data to 'chains'
for index, row in opstack_chains.iterrows():
    if all(row['defillama_slug'] != item[0] for item in chains):
        chains.append([row['defillama_slug'], row['chain_layer']])
# Iterate through the DataFrame and append data to 'chains'
for index, row in opstack_protocols.iterrows():
    if all(row['defillama_slug'] != item[0] for item in protocols):
        protocols.append([row['defillama_slug'], row['chain_layer'],row['chain_list']])


# In[ ]:


chain_name_list = [sublist[0] for sublist in chains]


# In[ ]:


print('get tvls')
p = dfl.get_all_protocol_tvls_by_chain_and_token(min_tvl=min_tvl_to_count_apps, chains = chain_name_list, do_aggregate = 'Yes')


# In[ ]:


# p
# print('gen flows')
# p = dfl.generate_flows_column(p)
# p = dfl.get_tvl(apistring= 'https://api.llama.fi/protocol/velodrome', chains = chain_name_list, prot = 'velodrome',prot_name='Velodrome')


# In[ ]:


# Aggregate data
app_dfl_list = p.groupby(['chain', 'date']).agg(
    distinct_protocol=pd.NamedAgg(column='protocol', aggfunc=pd.Series.nunique),
    distinct_parent_protocol=pd.NamedAgg(column='parent_protocol', aggfunc=pd.Series.nunique),
    sum_token_value_usd_flow=pd.NamedAgg(column='sum_token_value_usd_flow', aggfunc='sum'),
    sum_price_usd_flow=pd.NamedAgg(column='sum_price_usd_flow', aggfunc='sum'),
    sum_usd_value_check=pd.NamedAgg(column='sum_usd_value_check', aggfunc='sum')
)

p = None #Clear Memory

app_dfl_list = app_dfl_list.reset_index()
app_dfl_list = app_dfl_list.rename(columns={'chain':'defillama_slug'})


# display(app_dfl_list)
# app_dfl_list


# In[ ]:


p_agg = []
for p in protocols:
    try:
        d = dfl.get_single_tvl(p[0], p[2], print_api_str=False) #Set print_api_str=True for debugging
        # print(f"Raw content for {p}: {d}")
        d['chain_prot'] = p[0].title()
        d['layer'] = p[1]
        d['defillama_slug'] = 'protocols/' + p[0]
        d['source'] = 'protocol'
        # d['prot_chain'] = c
        p_agg.append(d)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for {p}: {e}")
        continue  # Continue to the next iteration of the loop
    except Exception as e:
        print(f"An unexpected error occurred for {p}: {e}")
        continue  # Continue to the next iteration of the loop

df = pd.concat(p_agg)
df = df.fillna(0)
# prob use total_app_tvl since it has more history and we're not yet doing flows
df_sum = df.groupby(['date', 'chain_prot', 'layer', 'defillama_slug', 'source','total_app_tvl']).agg({'usd_value':'sum'}).reset_index()
# print(df_sum.columns)
# display(df_sum)

df_sum = df_sum[['date', 'total_app_tvl', 'chain_prot', 'layer', 'defillama_slug', 'source']]
df_sum = df_sum.rename(columns={'chain_prot': 'chain', 'total_app_tvl': 'tvl'})

# print(df_sum.tail(5))


# In[ ]:


c_agg = []
for c in chains:
        try:
                d = dfl.get_historical_chain_tvl(c[0])
                d['chain'] = c[0]
                d['layer'] = c[1]
                d['defillama_slug'] = c[0]
                d['source'] = 'chain'
                
                c_agg.append(d)
        except Exception as e:
                print(f"An unexpected error occurred for {c}: {e}")
                continue  # Continue to the next iteration of the loop


df_ch = pd.concat(c_agg)
df = pd.concat([df_ch,df_sum])
# Rename
df['chain'] = df['chain'].str.replace('%20', ' ', regex=False)
df['chain'] = df['chain'].str.replace('-', ' ', regex=False)
df.loc[df['chain'] == 'Optimism', 'chain'] = 'OP Mainnet'
df.loc[df['chain'] == 'BSC', 'chain'] = 'BNB'
df.loc[df['chain'] == 'Brine.Fi', 'chain'] = 'Brine'
df.loc[df['chain'] == 'Immutablex', 'chain'] = 'ImmutableX'


df = df[df['date'] <= current_utc_date ] #rm dupes at current datetime

df['date'] = pd.to_datetime(df['date']) - timedelta(days=1) #map to the prior day, since dfl adds an extra day


# In[ ]:


df = df.merge(app_dfl_list, on =['defillama_slug','date'], how='left')
df.sample(5)


# In[ ]:


# Add Metadata
meta_cols = ['defillama_slug', 'is_op_chain','mainnet_chain_id','op_based_version', 'alignment','chain_name']

df = df.merge(opstack_metadata[meta_cols], on='defillama_slug', how = 'left')

df['alignment'] = df['alignment'].fillna('Other EVMs')


# In[ ]:


df[df['chain'] == 'Ethereum'].tail(5)


# In[ ]:


# export
folder = 'outputs/'
df.to_csv(folder + 'dfl_chain_tvl.csv', index = False)
# Write to Dune
du.write_dune_api_from_pandas(df, 'dfl_chain_tvl',\
                             'TVL for select chains from DefiLlama')

