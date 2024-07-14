#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('get qualified txs')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import flipside_utils as f
import clickhouse_utils as ch
import csv_utils as cu
import google_bq_utils as bqu
import opstack_metadata_utils as ops
sys.path.pop()

import time

import numpy as np
import pandas as pd
import os
import clickhouse_connect as cc


# In[ ]:


# d = ops.get_op_stack_metadata_by_data_source('flipside')
chain_configs = ops.generate_op_stack_chain_config_query_list()

# Get All Chain IDs in our metadata list & DB
# Commented out to read from Flip & CH responses
# chain_ids_string = ops.gen_chain_ids_list_for_param(chain_configs['mainnet_chain_id'])

# display(chain_configs)


# In[ ]:


ch_client = ch.connect_to_clickhouse_db() #Default is OPLabs DB

query_name = 'daily_evms_qualified_txs_counts'


# In[ ]:


col_list = [
        'dt','blockchain','name','layer','chain_id'
        , 'num_raw_txs', 'num_success_txs','num_qualified_txs','source'
        ]


# In[ ]:


trailing_days = 180

flipside_configs = chain_configs[chain_configs['source'] == 'flipside']
clickhouse_configs = chain_configs[chain_configs['source'] == 'oplabs']


# In[ ]:


print('     flipside runs')
flip_dfs = []

with open(os.path.join("inputs/sql/flipside_bychain.sql"), "r") as file:
    og_query = file.read()

for index, chain in flipside_configs.iterrows():
    print('     flipside: ' + chain['blockchain'])
    query = og_query
    # Pass in Params to the query
    query = query.replace("@blockchain@", chain['blockchain'])
    query = query.replace("@chain_id@", str(chain['mainnet_chain_id']))
    query = query.replace("@name@", chain['display_name'])
    query = query.replace("@layer@", chain['chain_layer'])
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


# In[ ]:


# Run Clickhouse
print('     clickhouse runs')
ch_dfs = []
with open(os.path.join("inputs/sql/goldsky_bychain.sql"), "r") as file:
                        og_query = file.read()

for index, chain in clickhouse_configs.iterrows():
        print(     'clickhouse: ' + chain['blockchain'])
        query = og_query
        #Pass in Params to the query
        query = query.replace("@blockchain@", chain['blockchain'])
        if chain['blockchain'] == 'bob':
                query = query.replace("chain_id, --db chain_id", str(chain['mainnet_chain_id']) + " as chain_id,")
                # query = query.replace("@chain_id@", str(chain['mainnet_chain_id']))
        query = query.replace("@name@", chain['display_name'])
        query = query.replace("@layer@", chain['chain_layer'])
        query = query.replace("@trailing_days@", str(trailing_days))
        try:
                df = ch_client.query_df(query)
                ch_dfs.append(df)
        except:
                print('unable to process ' + chain['blockchain'])

ch = pd.concat(ch_dfs)
ch['source'] = 'goldsky'
ch['dt'] = pd.to_datetime(ch['dt']).dt.tz_localize(None)
ch = ch[col_list]


# In[ ]:


# Get Chains we already have data for & don't need to run in Dune
dataframes = [ch, flip]

chain_ids_string = ops.get_unique_chain_ids_from_dfs(dataframes)


# In[ ]:


# Run Dune
print('     dune runs')
days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')
chain_ids_param = d.generate_query_parameter(input=chain_ids_string,field_name='list_chain_ids',dtype='text')

dune_df = d.get_dune_data(query_id = 3740822, #https://dune.com/queries/3740822
    name = "dune_evms_qualified_txs",
    path = "outputs",
    performance="large",
    params = [days_param,chain_ids_param],
    num_hours_to_rerun=0 #always rerun due to param
)
dune_df['source'] = 'dune'
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)
dune_df = dune_df[col_list]


# In[ ]:


# Combine dfs
final_df = pd.concat([ch, flip, dune_df])

# Remove Dupes
final_df = final_df.drop_duplicates(subset=['chain_id','dt'], keep='first')


# In[ ]:


opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')

opstack_metadata['chain_id'] = opstack_metadata['mainnet_chain_id']

meta_cols = ['is_op_chain','op_based_version', 'chain_id', 'alignment','chain_name', 'display_name']

print("Columns in opstack_metadata:", opstack_metadata.columns)
print("Columns in opstack_metadata[meta_cols]:", opstack_metadata[meta_cols].columns)
print("Columns in final_df:", final_df.columns)


# In[ ]:


final_enriched_df = final_df.merge(opstack_metadata[meta_cols], on='chain_id', how = 'left')
final_enriched_df['alignment'] = final_enriched_df['alignment'].fillna('Other EVMs')
final_enriched_df['is_op_chain'] = final_enriched_df['is_op_chain'].fillna(False)
final_enriched_df['display_name'] = final_enriched_df['display_name'].fillna(final_enriched_df['name'])

final_enriched_df = final_enriched_df.drop(columns=['name'])


# In[ ]:


final_enriched_df.sort_values(by=['dt','blockchain'], ascending =[False, False], inplace = True)

final_enriched_df.to_csv('outputs/'+query_name+'.csv', index=False)


# In[ ]:


final_enriched_df['chain_id'] = final_enriched_df['chain_id'].astype(int)
final_enriched_df['num_raw_txs'] = final_enriched_df['num_raw_txs'].astype(int)
final_enriched_df['num_success_txs'] = final_enriched_df['num_success_txs'].astype(int)
final_enriched_df['num_qualified_txs'] = final_enriched_df['num_qualified_txs'].fillna(0)
final_enriched_df['num_qualified_txs'] = final_enriched_df['num_qualified_txs'].astype(int)
# final_enriched_df.dtypes


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(final_enriched_df, query_name, unique_keys = ['chain_id','dt'])

