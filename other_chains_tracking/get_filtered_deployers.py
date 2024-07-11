#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('get filtered deployers')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
# import flipside_utils as f
# import clickhouse_utils as ch
import csv_utils as cu
import google_bq_utils as bqu
sys.path.pop()

import numpy as np
import pandas as pd
from datetime import timedelta
import os
# import clickhouse_connect as cc


# In[ ]:


# ch_client = ch.connect_to_clickhouse_db() #Default is OPLabs DB

query_name = 'daily_evms_filtered_deployers_counts'


# In[ ]:


trailing_days = 28 #Per Chain for Deployers

flipside_configs = [
        {'blockchain': 'blast', 'name': 'Blast', 'layer': 'L2'}
]
clickhouse_configs = [
        {'blockchain': 'metal', 'name': 'Metal', 'layer': 'L2'},
        {'blockchain': 'mode', 'name': 'Mode', 'layer': 'L2'},
        {'blockchain': 'bob', 'name': 'BOB (Build on Bitcoin)', 'layer': 'L2'},
        {'blockchain': 'fraxtal', 'name': 'Fraxtal', 'layer': 'L2'},
        {'blockchain': 'cyber', 'name': 'Cyber', 'layer': 'L2'},
        {'blockchain': 'mint', 'name': 'Mint', 'layer': 'L2'},
]


# In[ ]:


# # Run Flipside - TODO: Build Deployer Query (tbd if it will run)
# print('     flipside runs')
# flip_dfs = []
# with open(os.path.join("inputs/sql/flipside_bychain.sql"), "r") as file:
#                         og_query = file.read()

# for chain in flipside_configs:
#         print(     'flipside: ' + chain['blockchain'])
#         query = og_query
#         #Pass in Params to the query
#         query = query.replace("@blockchain@", chain['blockchain'])
#         query = query.replace("@name@", chain['name'])
#         query = query.replace("@layer@", chain['layer'])
#         query = query.replace("@trailing_days@", str(trailing_days))
        
#         df = f.query_to_df(query)

#         flip_dfs.append(df)

# flip = pd.concat(flip_dfs)
# flip['source'] = 'flipside'
# flip['dt'] = pd.to_datetime(flip['dt']).dt.tz_localize(None)
# flip = flip[['dt','blockchain','name','layer','num_qualified_txs','source']]


# In[ ]:


# Run Dune
print('     dune runs')
days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')
deploy_dune_df = d.get_dune_data(query_id = 3753590, #https://dune.com/queries/3753590
    name = "daily_evms_filtered_deployers_dune",
    path = "outputs",
    performance="large",
    params = [days_param]
)
revdev_dune_df = d.get_dune_data(query_id = 3329567, #https://dune.com/queries/3329567
    name = "daily_evms_revdevs_dune",
    path = "outputs"#,
    # parameters = [days_param]
)

deploy_dune_df['source'] = 'dune'
revdev_dune_df['source'] = 'dune'

deploy_dune_df['created_dt'] = pd.to_datetime(deploy_dune_df['created_dt']).dt.tz_localize(None)
revdev_dune_df['dt'] = pd.to_datetime(revdev_dune_df['dt']).dt.tz_localize(None)
revdev_dune_df = revdev_dune_df.rename(columns={'chain':'blockchain'})

# deploy_dune_df = deploy_dune_df[['created_dt','blockchain','creator_address']]
# revdev_dune_df = revdev_dune_df[['created_dt','blockchain','creator_address']]


# In[ ]:


# deploy_dune_df


# In[ ]:


dune_meta_df = d.get_dune_data(query_id = 3445473, #https://dune.com/queries/3445473
    name = "dune_evms_info",
    path = "outputs",
    num_hours_to_rerun = 12
)
dune_meta_df = dune_meta_df.rename(columns={'dune_schema':'blockchain'})
cols = ['name','layer','chain_id']
deploy_dune_df = deploy_dune_df.merge(dune_meta_df[cols], on='chain_id',how='inner')
revdev_dune_df = revdev_dune_df.merge(dune_meta_df[cols], on='chain_id',how='left')

# deploy_dune_df.sample(5)


# In[ ]:


# revdev_dune_df.head()


# In[ ]:


# # Run Clickhouse - TODO: Build Deployer Query (tbd if it will run)
# print('     clickhouse runs')
# ch_dfs = []
# with open(os.path.join("inputs/sql/goldsky_bychain.sql"), "r") as file:
#                         og_query = file.read()

# for chain in clickhouse_configs:
#         print(     'clickhouse: ' + chain['blockchain'])
#         query = og_query
#         #Pass in Params to the query
#         query = query.replace("@blockchain@", chain['blockchain'])
#         query = query.replace("@name@", chain['name'])
#         query = query.replace("@layer@", chain['layer'])
#         query = query.replace("@trailing_days@", str(trailing_days))
        
#         df = ch_client.query_df(query)

#         ch_dfs.append(df)

# ch = pd.concat(ch_dfs)
# ch['source'] = 'goldsky'
# ch['dt'] = pd.to_datetime(ch['dt']).dt.tz_localize(None)
# ch = ch[['dt','blockchain','name','layer','num_qualified_txs','source']]


# In[ ]:


# Step 1: Filter dune_df for chains not in flip
# filtered_dune_df = dune_df[~dune_df['blockchain'].isin(flip['blockchain'])]
# Step 2: Union flip and filtered_dune_df
# combined_flip_dune = pd.concat([flip, filtered_dune_df])
# # Step 3: Filter ch for chains not in combined_flip_dune
# filtered_ch = ch[~ch['blockchain'].isin(combined_flip_dune['blockchain'])]
# # Step 4: Union the result with filtered_ch
# final_df = pd.concat([combined_flip_dune, filtered_ch])
# # final_df
unified_deployers_df = deploy_dune_df
unified_revdev_df = revdev_dune_df


# In[ ]:


opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')
# Filter for rows where is_op_chain is True and dune_schema_name is not null
op_chains_df = opstack_metadata[(opstack_metadata['is_op_chain']) & (opstack_metadata['dune_schema'].notnull())]
# Get the unique entries in dune_schema_name
op_chains = op_chains_df['dune_schema'].unique().tolist()

op_chains
# op_chains = opstac


# In[ ]:


unified_deployers_df.sample(5)


# In[ ]:


# Ensure created_dt is in datetime format
unified_deployers_df['created_dt'] = pd.to_datetime(unified_deployers_df['created_dt'])

# Generate a date range for the period you want to analyze
start_date = unified_deployers_df['created_dt'].min()
end_date = unified_deployers_df['created_dt'].max()

date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Initialize a list to store results
results = []

# Iterate over each date in the range
for single_date in date_range:
    # Define the date window
    window_start = single_date - timedelta(days=28)
    window_end = single_date
    
    # Filter the dataframe for the current window
    window_df = unified_deployers_df[
        (unified_deployers_df['created_dt'] >= window_start) &
        (unified_deployers_df['created_dt'] <= window_end)
    ]
    
    # Group by blockchain and count unique creator_addresses
    unique_counts = window_df.groupby(['blockchain','name','layer','chain_id'])['creator_address'].nunique().reset_index()
    unique_counts['dt'] = single_date
    # Append the individual blockchain results
    results.append(unique_counts)

    # Calculate the 'All' unique count
    all_count = window_df['creator_address'].nunique()
    results.append(pd.DataFrame({
        'blockchain': ['all'],
        'name': ['All'],
        'layer': ['Aggregate'],
        'chain_id': [None],
        'creator_address': [all_count],
        'dt': [single_date]
    }))
    # Calculate the 'OP Chains' unique count
    layers = ['L1','L2','L3']
    for i in layers:
        l_window_df = window_df[window_df['layer'] == i]
        l_count = l_window_df['creator_address'].nunique()
        results.append(pd.DataFrame({
            'blockchain': [i.lower()],
            'name': [i + 's'],
            'layer': ['Aggregate'],
            'chain_id': [None],
            'creator_address': [l_count],
            'dt': [single_date]
        }))
    # Calculate the 'OP Chains' unique count
    op_window_df = window_df[window_df['blockchain'].isin(op_chains)]
    op_count = op_window_df['creator_address'].nunique()
    results.append(pd.DataFrame({
        'blockchain': ['op chains'],
        'name': ['OP Chains'],
        'layer': ['Aggregate'],
        'chain_id': [None],
        'creator_address': [op_count],
        'dt': [single_date]
    }))

# Concatenate all results into a single DataFrame
final_df = pd.concat(results)

# Optional: Reset index for better readability
final_df = final_df.reset_index(drop=True)

# print(final_df)


# In[ ]:


final_df.sample(5)


# In[ ]:


meta_cols = ['is_op_chain','mainnet_chain_id','op_based_version', 'alignment','chain_name', 'display_name']
opstack_metadata_map = opstack_metadata[meta_cols]
opstack_metadata_map = opstack_metadata_map.rename(columns={'mainnet_chain_id':'chain_id'})

opstack_metadata_map = opstack_metadata_map[opstack_metadata_map['chain_id'].notnull()]
opstack_metadata_map.sample(5)


# In[ ]:


# final_df


# In[ ]:


deployer_enriched_df = final_df.merge(opstack_metadata_map, on='chain_id', how = 'left')

deployer_enriched_df['alignment'] = deployer_enriched_df['alignment'].fillna('Other EVMs')
deployer_enriched_df.loc[deployer_enriched_df['layer'] == 'Aggregate', 'alignment'] = 'Aggregate'

deployer_enriched_df['is_op_chain'] = deployer_enriched_df['is_op_chain'].fillna(False)
deployer_enriched_df['display_name'] = deployer_enriched_df['display_name'].fillna(deployer_enriched_df['name'])

deployer_enriched_df = deployer_enriched_df.drop(columns=['name'])


# In[ ]:


deployer_enriched_df.sample(5)


# In[ ]:


unified_revdev_df = unified_revdev_df.drop(['is_op_chain','chain_name'],axis=1)

revdev_enriched_df = unified_revdev_df.merge(opstack_metadata_map, on='chain_id', how = 'left')
revdev_enriched_df['alignment'] = revdev_enriched_df['alignment'].fillna('Other EVMs')
revdev_enriched_df['is_op_chain'] = revdev_enriched_df['is_op_chain'].fillna(False)
revdev_enriched_df['display_name'] = revdev_enriched_df['display_name'].fillna(revdev_enriched_df['name'])
revdev_enriched_df = revdev_enriched_df.drop(columns=['name'])


# In[ ]:


# List of DataFrames
dataframes = [deployer_enriched_df, revdev_enriched_df, unified_deployers_df]

# Process each DataFrame
for df in dataframes:
    df['blockchain'] = df['blockchain'].astype(str).fillna('-').str.strip()
    df['chain_id'] = df['chain_id'].fillna(-1)
    df.reset_index(drop=True, inplace=True)


# In[ ]:


# Check DataFrame information to verify data types and non-null counts
# print(deployer_enriched_df.info())
# print(revdev_enriched_df.info())
# print(unified_deployers_df.info())


# In[ ]:


deployer_enriched_df.sort_values(by=['dt','blockchain'], ascending =[False, False], inplace = True)
deployer_enriched_df.to_csv('outputs/daily_filter_deployer_counts.csv', index=False)

revdev_enriched_df.sort_values(by=['dt','blockchain'], ascending =[False, False], inplace = True)
revdev_enriched_df.to_csv('outputs/daily_revdev_counts.csv', index=False)


# In[ ]:


# print(revdev_enriched_df.dtypes)


# In[ ]:


# revdev_enriched_df.info()


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(deployer_enriched_df, 'daily_filter_deployer_counts',unique_keys=['dt','blockchain'])
bqu.append_and_upsert_df_to_bq_table(revdev_enriched_df, 'daily_revdev_counts',unique_keys=['dt','blockchain'])
# Raw Deployer Address Data
bqu.append_and_upsert_df_to_bq_table(unified_deployers_df, 'daily_filter_deployer_address_list', unique_keys = ['blockchain','created_dt','creator_address'])

