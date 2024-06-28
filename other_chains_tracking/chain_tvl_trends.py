#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start tvl trends')
import pandas as pd
import sys
import numpy as np
import json

import time

from datetime import datetime, timedelta
sys.path.append("../helper_functions")
import defillama_utils as dfl
import duneapi_utils as du
import opstack_metadata_utils as ops
import csv_utils as cu
import google_bq_utils as bqu
sys.path.pop()

current_utc_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
# print(current_utc_date)


# In[ ]:


categorized_chains = dfl.get_chains_config()


# In[ ]:


categorized_chains.to_csv('outputs/dfl_categorized_chains.csv', index=False)


# In[ ]:


# Convert float values in 'parent' column to dictionaries
categorized_chains['parent'] = categorized_chains['parent'].apply(lambda x: {} if pd.isna(x) else x)

# Check if the chain belongs to EVM category
categorized_chains['is_EVM'] = categorized_chains['categories'].apply(lambda x: isinstance(x, list) and 'EVM' in x)

# Check if the chain belongs to Rollup category or L2 is in parent_types
def is_rollup(row):
    categories = row['categories']
    parent = row.get('parent')
    parent_types = parent.get('types') if parent else []
    
    if isinstance(categories, list) and 'Rollup' in categories:
        return True
    if 'L2' in parent_types:
        return True
    
    return False

def extract_layer(row):
    parent = row.get('parent')
    parent_types = parent.get('types') if parent else []
    
    if 'L2' in parent_types:
        return 'L2'
    elif 'L3' in parent_types:
        return 'L3'
    
    return 'L1'

categorized_chains['is_Rollup'] = categorized_chains.apply(is_rollup, axis=1)
categorized_chains['layer'] = categorized_chains.apply(extract_layer, axis=1)

#Replace opBNB
categorized_chains['defillama_slug'] = categorized_chains['defillama_slug'].replace('Op_Bnb', 'opBNB')

considered_chains = categorized_chains[categorized_chains['is_EVM']]
considered_chains = considered_chains[['defillama_slug','layer','is_EVM','is_Rollup','chainId']]

# considered_chains


# In[ ]:


min_tvl_to_count_apps = 0


# In[ ]:


# OPStack Metadata will auto pull, but we can also add curated protocols here (we handle for dupes)
curated_chains = [
    #L2s
     ['Optimism', 'L2']
    ,['Base', 'L2']
    ,['Arbitrum', 'L2']
    #non-dune L2s - Check L2Beat for comprehensiveness
    ,['zkSync Era', 'L2']
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
    ,['Arbitrum Nova','L2']
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
tvl_opstack_metadata = tvl_opstack_metadata[['defillama_slug','chain_layer','chain_type']]

opstack_chains = tvl_opstack_metadata[~tvl_opstack_metadata['defillama_slug'].str.contains('protocols/')].copy()
op_superchain_chains = tvl_opstack_metadata[tvl_opstack_metadata['chain_type'].notna()].copy()
opstack_protocols = tvl_opstack_metadata[tvl_opstack_metadata['defillama_slug'].str.contains('protocols/')].copy()
#clean column
opstack_protocols['defillama_slug'] = opstack_protocols['defillama_slug'].str.replace('protocols/','')
# Use apply to create an array with one element
opstack_protocols['chain_list'] = opstack_protocols.apply(lambda x: ['ethereum'], axis=1) #guess, we can manually override if not


# In[ ]:


print(op_superchain_chains)


# In[ ]:


# Create new lists
curated_chains_tmp = curated_chains.copy()
protocols = curated_protocols.copy()
# Iterate through the DataFrame and append data to 'chains'
for index, row in opstack_chains.iterrows():
    if all(row['defillama_slug'] != item[0] for item in curated_chains_tmp):
        curated_chains_tmp.append([row['defillama_slug'], row['chain_layer']])
# Iterate through the DataFrame and append data to 'chains'
for index, row in opstack_protocols.iterrows():
    if all(row['defillama_slug'] != item[0] for item in protocols):
        protocols.append([row['defillama_slug'], row['chain_layer'],row['chain_list']])


# In[ ]:


raw_chains_df = pd.DataFrame(curated_chains_tmp, columns = ['defillama_slug','layer'])
chains_df = raw_chains_df.merge(considered_chains, on = ['defillama_slug','layer'], how = 'left')
chains_df.fillna({'is_EVM':True}, inplace = True)
chains_df.fillna({'is_Rollup':True}, inplace = True)


# In[ ]:


# chains_df


# In[ ]:


# Get the chains already present in chains_df
existing_chains = set(chains_df['defillama_slug'])

# Filter out the chains from considered_chains that are not in chains_df
to_append = considered_chains[~considered_chains['defillama_slug'].isin(existing_chains)]

# Append the selected rows to chains_df
chains_df = pd.concat([chains_df, to_append], ignore_index=True)


# merged_chains_df.sort_values(by='chain').head(20)


# In[ ]:


chain_name_list = chains_df['defillama_slug'].unique().tolist()
get_app_list = op_superchain_chains['defillama_slug'].unique().tolist()


# In[ ]:


print('get tvls')
p = dfl.get_all_protocol_tvls_by_chain_and_token(min_tvl=min_tvl_to_count_apps, chains = get_app_list, do_aggregate = 'Yes')


# In[ ]:


# p
# print('gen flows')
# p = dfl.generate_flows_column(p)
# p = dfl.get_tvl(apistring= 'https://api.llama.fi/protocol/velodrome', chains = chain_name_list, prot = 'velodrome',prot_name='Velodrome')


# In[ ]:


# Aggregate data
# TODO: Try to use config API to get Apps by Chain, since this only shows TVLs

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


# display(app_dfl_list[app_dfl_list['defillama_slug']=='Mode'])


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
df_sum = df.groupby(['date', 'chain_prot', 'layer', 'defillama_slug', 'source']).agg({'usd_value':'sum'}).reset_index()
# print(df_sum.columns)

df_sum = df_sum[['date', 'usd_value', 'chain_prot', 'layer', 'defillama_slug', 'source']]
df_sum = df_sum.rename(columns={'chain_prot': 'chain', 'usd_value': 'tvl'})

# print(df_sum.tail(5))


# In[ ]:


# display(df_sum[df_sum['chain']=='Aevo'])
# display(df_sum)


# In[ ]:


c_agg = []
for c in curated_chains_tmp:
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
df['tvl'] = df['tvl'].fillna(0)
df['tvl'] = df['tvl'].replace('', 0)

df['date'] = pd.to_datetime(df['date']) - timedelta(days=1) #map to the prior day, since dfl adds an extra day


# In[ ]:


df = df.merge(app_dfl_list, on =['defillama_slug','date'], how='left')


# In[ ]:


# display(df[df['chain']=='Mode'])


# In[ ]:


# Add Metadata
meta_cols = ['defillama_slug', 'is_op_chain','mainnet_chain_id','op_based_version', 'alignment','chain_name','display_name']

df = df.merge(opstack_metadata[meta_cols], on='defillama_slug', how = 'left')

df['alignment'] = df['alignment'].fillna('Other EVMs')
df['is_op_chain'] = df['is_op_chain'].fillna(False)
df['date'] = pd.to_datetime(df['date'])
df['tvl'].fillna(0, inplace=True)  # Or use an appropriate default value

#DFL Metadata cols
df = df.merge(chains_df, on = ['defillama_slug'], how = 'left')

# Coalesce layer_x and layer_y into a new column layer
df['layer'] = df['layer_x'].combine_first(df['layer_y'])

# Drop the original layer_x and layer_y columns
df.drop(['layer_x', 'layer_y'], axis=1, inplace=True)


# In[ ]:


# Convert the column to string
df['is_EVM'] = df['is_EVM'].astype(str)
df['is_Rollup'] = df['is_Rollup'].astype(str)
df['chainId'] = df['chainId'].fillna(-1).astype(int)


# In[ ]:


cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=365)
# Filter the DataFrame to the last 365 days
df_365 = df[df['date'] >= cutoff_date]
# df_365.sample(5)


# In[ ]:


#  Define aggregation functions for each column
aggregations = {
    'tvl': ['min', 'last', 'mean'],
    'distinct_protocol': ['min', 'last', 'mean'],
    'distinct_parent_protocol': ['min', 'last', 'mean'],
    'sum_token_value_usd_flow': 'sum',
    'sum_price_usd_flow': 'sum',
    'sum_usd_value_check': 'sum'
}
# Function to perform aggregation based on frequency
def aggregate_data(df, freq, date_col='date', groupby_cols=None, aggs=None):
    if groupby_cols is None:
        groupby_cols = ['chain', 'layer', 'defillama_slug', 'source', 'is_op_chain', 'mainnet_chain_id', 'op_based_version', 'alignment', 'chain_name', 'display_name', 'chainId']
    if aggs is None:
        aggs = aggregations

    # Group by the specified frequency and other columns, then apply aggregations
    df_agg = df.groupby([pd.Grouper(key=date_col, freq=freq, closed='left')] + groupby_cols, dropna=False).agg(aggs).reset_index()
    # Flatten the hierarchical column index and concatenate aggregation function names with column names
    df_agg.columns = [f'{col}_{func}' if func != '' else col for col, func in df_agg.columns]
    # Rename the 'date' column based on the frequency
    date_col_name = 'month' if freq == 'MS' else 'week'
    df_agg.rename(columns={f'{date_col}_': date_col_name}, inplace=True)
    df_agg['date'] = pd.to_datetime(df_agg['date'])
    
    return df_agg

# Perform monthly aggregation
df_monthly = aggregate_data(df, freq='MS')

# Perform weekly aggregation
df_weekly = aggregate_data(df, freq='W-MON')

# Now df_monthly and df_weekly contain the aggregated data


# In[ ]:


# df[df['chain'] == 'Ethereum'].tail(5)
# df_monthly.sample(10)
# df_weekly.sample(10)


# In[ ]:


# export
folder = 'outputs/'
df.to_csv(folder + 'dfl_chain_tvl.csv', index = False)
df_365.to_csv(folder + 'dfl_chain_tvl_t365d.csv', index = False)
df_monthly.to_csv(folder + 'dfl_chain_tvl_monthly.csv', index = False)
df_weekly.to_csv(folder + 'dfl_chain_tvl_weekly.csv', index = False)
# Write to Dune
du.write_dune_api_from_pandas(df, 'dfl_chain_tvl',\
                             'TVL for select chains from DefiLlama')
du.write_dune_api_from_pandas(df_monthly, 'dfl_chain_tv_monthly',\
                             'Monthly TVL for select chains from DefiLlama')
du.write_dune_api_from_pandas(df_weekly, 'dfl_chain_tv_weekly',\
                             'Weekly TVL for select chains from DefiLlama')


# In[ ]:


#BQ Upload
bqu.write_df_to_bq_table(df, 'daily_defillama_chain_tvl')
time.sleep(1)
bqu.write_df_to_bq_table(df_monthly, 'monthly_defillama_chain_tvl')
time.sleep(1)
bqu.write_df_to_bq_table(df_weekly, 'weekly_defillama_chain_tvl')

