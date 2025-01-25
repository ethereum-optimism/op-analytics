#!/usr/bin/env python
# coding: utf-8

# In[1]:


# ! pipenv run jupyter nbconvert --to python get_all_txs.ipynb


# In[2]:


print('get all txs')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import google_bq_utils as bqu
sys.path.pop()

import time
import datetime

import numpy as np
import pandas as pd
import os


# In[3]:


col_list = [
        'dt','blockchain','name','layer','chain_id'
        , 'num_raw_txs', 'num_success_txs','num_qualified_txs','source'
        ]
query_name = 'dune_all_txs'
rerun_hrs = 4


# In[4]:


chain_config = [
    #blockchain, display_name, count_func, gas_field, transactions_table
    ['bitcoin','Bitcoin','COUNT_IF(fee > 0)','COUNT(DISTINCT block_height)','transactions','fee','BTC'],
    ['near','Near','COUNT(distinct case when gas_price > 0 then tx_hash else null end)','COUNT(DISTINCT block_height)','actions','cast(NULL as double)','NULL'],
    ['aptos','Aptos','COUNT_IF(gas_used>0)','COUNT(DISTINCT block_height)','user_transactions','(gas_used*gas_unit_price/1e8)','APT'],
    # ['stellar','Stellar','COUNT_IF(fee_charged>0)','COUNT(DISTINCT block_height)','fee_charged','history_transactions'], --not date partitioned
    ['kaia','Kaia','COUNT_IF(gas_price>0)','COUNT(DISTINCT block_number)','transactions','cast(NULL as double)','NULL'],
    ['ton','TON','COUNT_IF(compute_gas_fees>0)','COUNT(DISTINCT block_seqno)','transactions','cast(NULL as double)','NULL'],
]


# In[5]:


trailing_days = 990


# In[6]:


days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')


# In[7]:


# Run Dune
print('     dune runs')
fees_df = d.get_dune_data(query_id = 4229341, #https://dune.com/queries/4229341
    name = query_name,
    path = "outputs",
    performance="large",
    params = [days_param],
    num_hours_to_rerun=rerun_hrs
)


# In[8]:


fees_df.sample(5)


# In[9]:


unique_blockchains = fees_df['blockchain'].unique().tolist()


# In[10]:


chain_dfs = []
for row in chain_config:
    blockchain = row[0]
    print(blockchain)
    if blockchain in unique_blockchains:
        continue
    else:
        #blockchain, display_name, count_func, gas_field, transactions_table
        blockchain_param = d.generate_query_parameter(input=blockchain,field_name='blockchain',dtype='text')
        display_name_param = d.generate_query_parameter(input=row[1],field_name='display_name',dtype='text')
        count_func_param = d.generate_query_parameter(input=row[2],field_name='count_func',dtype='text')
        count_block_func_param = d.generate_query_parameter(input=row[3],field_name='count_block_func',dtype='text')
        # gas_field_param = d.generate_query_parameter(input=row[4],field_name='gas_field',dtype='text')
        transactions_table_param = d.generate_query_parameter(input=row[4],field_name='transactions_table',dtype='text')
        tx_fee_func_param = d.generate_query_parameter(input=row[5],field_name='tx_fee_func_internal',dtype='text')
        tx_fee_currency_param = d.generate_query_parameter(input=row[6],field_name='tx_fee_currency',dtype='text')

        chain_df = d.get_dune_data(query_id = 4230061, #https://dune.com/queries/4230061
            name = query_name + '_by_chain',
            path = "outputs",
            performance="large",
            params = [days_param,blockchain_param,display_name_param,count_func_param,count_block_func_param,transactions_table_param,tx_fee_func_param,tx_fee_currency_param],
            num_hours_to_rerun=rerun_hrs
        )
        # print(chain_df.sample(3))
        chain_dfs.append(chain_df)

chain_df_agg = pd.concat(chain_dfs)


# In[11]:


print(fees_df.columns)
print(chain_df_agg.columns)


# In[12]:


dune_df = pd.concat([fees_df,chain_df_agg])


# In[13]:


if not dune_df.empty:
    dune_df['source'] = 'dune'
    dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)
    dune_df = dune_df[dune_df['dt'].dt.date < datetime.datetime.now(datetime.timezone.utc).date()]
    dune_df['chain_id'] = dune_df['chain_id'].astype(str)
    dune_df['chain_id'] = dune_df['chain_id'].astype(str).str.replace(r'\.0$', '', regex=True)


# In[14]:


# Verify that all elements are strings
assert dune_df['chain_id'].apply(type).eq(str).all(), "Not all elements are strings"
print(dune_df['chain_id'].dtype)


# In[15]:


# dune_df.sample(5)
print(dune_df.dtypes)


# In[16]:


print(f"dune_df shape: {dune_df.shape}")


# In[17]:


unique_cols = ['blockchain', 'dt','tx_fee_currency']


# In[18]:


# Identify duplicate combinations
duplicates = dune_df.duplicated(subset=unique_cols, keep=False)

# View the duplicate rows
duplicate_rows = dune_df[duplicates]

# Display the duplicate rows
print(duplicate_rows)

# Get a count of duplicates for each combination
duplicate_counts = dune_df.groupby(unique_cols).size().reset_index(name='count')
duplicate_counts = duplicate_counts[duplicate_counts['count'] > 1]

print("\nDuplicate combination counts:")
print(duplicate_counts)


# In[19]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, query_name, unique_keys = unique_cols)

