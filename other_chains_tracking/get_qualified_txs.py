#!/usr/bin/env python
# coding: utf-8

# In[1]:


print('get qualified txs')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import flipside_utils as f
import clickhouse_utils as ch
import csv_utils as cu
import google_bq_utils as bqu
sys.path.pop()

import time

import numpy as np
import pandas as pd
import os
import clickhouse_connect as cc


# In[2]:


ch_client = ch.connect_to_clickhouse_db() #Default is OPLabs DB

query_name = 'daily_evms_qualified_txs_counts'


# In[3]:


col_list = [
        'dt','blockchain','name','layer','chain_id'
        , 'num_raw_txs', 'num_success_txs','num_qualified_txs','source'
        ]


# In[16]:


trailing_days = 90
flipside_configs = [
        {'blockchain': 'blast', 'chain_id': 238, 'name': 'Blast', 'layer': 'L2'}
]
clickhouse_configs = [
        {'blockchain': 'metal', 'chain_id': 1750,'name': 'Metal', 'layer': 'L2'},
        {'blockchain': 'mode', 'chain_id': 34443,'name': 'Mode', 'layer': 'L2'},
        {'blockchain': 'bob', 'chain_id': 60808,'name': 'BOB (Build on Bitcoin)', 'layer': 'L2'},
        {'blockchain': 'fraxtal', 'chain_id': 252,'name': 'Fraxtal', 'layer': 'L2'},
        {'blockchain': 'cyber', 'chain_id': 7560,'name': 'Cyber', 'layer': 'L2'},
        {'blockchain': 'mint', 'chain_id': 185,'name': 'Mint', 'layer': 'L2'},
]


# In[5]:


print('     flipside runs')
flip_dfs = []
with open(os.path.join("inputs/sql/flipside_bychain.sql"), "r") as file:
    og_query = file.read()

for chain in flipside_configs:
    print('     flipside: ' + chain['blockchain'])
    query = og_query
    # Pass in Params to the query
    query = query.replace("@blockchain@", chain['blockchain'])
    query = query.replace("@chain_id@", str(chain['chain_id']))
    query = query.replace("@name@", chain['name'])
    query = query.replace("@layer@", chain['layer'])
    query = query.replace("@trailing_days@", str(trailing_days))
    
    try:
        df = f.query_to_df(query)
        flip_dfs.append(df)
    except Exception as e:  # Use FlipsideError if available instead of Exception
        print(f"Error querying Flipside for {chain['blockchain']}: {str(e)}")
        print("Skipping this chain due to API credit limitation or other issues.")
        continue

if flip_dfs:
    flip = pd.concat(flip_dfs)
    flip['source'] = 'flipside'
    flip['dt'] = pd.to_datetime(flip['dt']).dt.tz_localize(None)
    flip = flip[col_list]
else:
    print("No data was retrieved from Flipside. The resulting DataFrame will be empty.")
    flip = pd.DataFrame(columns=col_list)


# In[6]:


# Run Dune
print('     dune runs')
days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')
dune_df = d.get_dune_data(query_id = 3740822, #https://dune.com/queries/3740822
    name = "dune_evms_qualified_txs",
    path = "outputs",
    performance="large",
    params = [days_param],
    num_hours_to_rerun=12
)
dune_df['source'] = 'dune'
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)
dune_df = dune_df[col_list]


# In[17]:


# Run Clickhouse
print('     clickhouse runs')
ch_dfs = []
with open(os.path.join("inputs/sql/goldsky_bychain.sql"), "r") as file:
                        og_query = file.read()

for chain in clickhouse_configs:
        print(     'clickhouse: ' + chain['blockchain'])
        query = og_query
        #Pass in Params to the query
        query = query.replace("@blockchain@", chain['blockchain'])
        if chain['blockchain'] == 'bob':
                query = query.replace("chain_id, --db chain_id", str(chain['chain_id']) + " as chain_id,")
                # query = query.replace("@chain_id@", str(chain['chain_id']))
        query = query.replace("@name@", chain['name'])
        query = query.replace("@layer@", chain['layer'])
        query = query.replace("@trailing_days@", str(trailing_days))
        
        df = ch_client.query_df(query)

        ch_dfs.append(df)

ch = pd.concat(ch_dfs)
ch['source'] = 'goldsky'
ch['dt'] = pd.to_datetime(ch['dt']).dt.tz_localize(None)
ch = ch[col_list]


# In[10]:


# Step 1: Filter dune_df for chains not in flip
filtered_dune_df = dune_df[~dune_df['chain_id'].isin(flip['chain_id'])]
# Step 2: Union flip and filtered_dune_df
combined_flip_dune = pd.concat([flip, filtered_dune_df])
# Step 3: Filter ch for chains not in combined_flip_dune
filtered_ch = ch[~ch['chain_id'].isin(combined_flip_dune['chain_id'])]
# Step 4: Union the result with filtered_ch
final_df = pd.concat([combined_flip_dune, filtered_ch])
# final_df


# In[11]:


opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')

opstack_metadata['chain_id'] = opstack_metadata['mainnet_chain_id']

meta_cols = ['is_op_chain','op_based_version', 'chain_id', 'alignment','chain_name', 'display_name']

print("Columns in opstack_metadata:", opstack_metadata.columns)
print("Columns in opstack_metadata[meta_cols]:", opstack_metadata[meta_cols].columns)
print("Columns in final_df:", final_df.columns)


# In[12]:


final_enriched_df = final_df.merge(opstack_metadata[meta_cols], on='chain_id', how = 'left')
final_enriched_df['alignment'] = final_enriched_df['alignment'].fillna('Other EVMs')
final_enriched_df['is_op_chain'] = final_enriched_df['is_op_chain'].fillna(False)
final_enriched_df['display_name'] = final_enriched_df['display_name'].fillna(final_enriched_df['name'])

final_enriched_df = final_enriched_df.drop(columns=['name'])


# In[13]:


final_enriched_df.sort_values(by=['dt','blockchain'], ascending =[False, False], inplace = True)

final_enriched_df.to_csv('outputs/'+query_name+'.csv', index=False)


# In[14]:


final_enriched_df['chain_id'] = final_enriched_df['chain_id'].astype(int)
final_enriched_df['num_raw_txs'] = final_enriched_df['num_raw_txs'].astype(int)
final_enriched_df['num_success_txs'] = final_enriched_df['num_success_txs'].astype(int)
final_enriched_df['num_qualified_txs'] = final_enriched_df['num_qualified_txs'].astype(int)
# final_enriched_df.dtypes


# In[15]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(final_enriched_df, query_name, unique_keys = ['chain_id','dt'])

