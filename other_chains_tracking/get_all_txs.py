#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ! pipenv run jupyter nbconvert --to python get_all_txs.ipynb


# In[ ]:


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


# In[2]:


col_list = [
        'dt','blockchain','name','layer','chain_id'
        , 'num_raw_txs', 'num_success_txs','num_qualified_txs','source'
        ]
query_name = 'dune_all_txs'
rerun_hrs = 4


# In[3]:


chain_config = [
    #blockchain, display_name, count_func, gas_field, transactions_table
    ['bitcoin','Bitcoin','count(*)','COUNT(DISTINCT block_height)','fee','transactions'],
    ['near','Near','count(distinct tx_hash)','COUNT(DISTINCT block_height)','gas_price','actions'],
    ['aptos','Aptos','count(*)','COUNT(DISTINCT block_height)','gas_used','transactions'],
    # ['stellar','Stellar','count(*)','COUNT(DISTINCT block_height)','fee_charged','history_transactions'], --not date partitioned
    ['kaia','Kaia','count(*)','COUNT(DISTINCT block_number)','gas_price','transactions'],
    ['ton','TON','count(*)','COUNT(DISTINCT block_seqno)','compute_gas_fees','transactions'],
]


# In[4]:


trailing_days = 60


# In[5]:


days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')


# In[ ]:


# Run Dune
print('     dune runs')
fees_df = d.get_dune_data(query_id = 4229341, #https://dune.com/queries/4229341
    name = query_name,
    path = "outputs",
    performance="large",
    params = [days_param],
    num_hours_to_rerun=rerun_hrs
)


# In[ ]:


fees_df.sample(5)


# In[8]:


unique_blockchains = fees_df['blockchain'].unique().tolist()


# In[ ]:


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
        gas_field_param = d.generate_query_parameter(input=row[4],field_name='gas_field',dtype='text')
        transactions_table_param = d.generate_query_parameter(input=row[5],field_name='transactions_table',dtype='text')

        chain_df = d.get_dune_data(query_id = 4230061, #https://dune.com/queries/4230061
            name = query_name + '_by_chain',
            path = "outputs",
            performance="large",
            params = [days_param,blockchain_param,display_name_param,count_func_param,count_block_func_param,gas_field_param,transactions_table_param],
            num_hours_to_rerun=rerun_hrs
        )
        # print(chain_df.sample(3))
        chain_dfs.append(chain_df)

chain_df_agg = pd.concat(chain_dfs)


# In[ ]:


print(fees_df.columns)
print(chain_df_agg.columns)


# In[ ]:


dune_df = pd.concat([fees_df,chain_df_agg])


# In[13]:


if not dune_df.empty:
    dune_df['source'] = 'dune'
    dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)
    dune_df = dune_df[dune_df['dt'].dt.date < datetime.datetime.now(datetime.timezone.utc).date()]
    dune_df['chain_id'] = dune_df['chain_id'].astype(str)
    dune_df['chain_id'] = dune_df['chain_id'].astype(str).str.replace(r'\.0$', '', regex=True)


# In[ ]:


# Verify that all elements are strings
assert dune_df['chain_id'].apply(type).eq(str).all(), "Not all elements are strings"
print(dune_df['chain_id'].dtype)


# In[ ]:


# dune_df.sample(5)
print(dune_df.dtypes)


# In[ ]:


print(f"dune_df shape: {dune_df.shape}")


# In[17]:


unique_cols = ['blockchain', 'dt','tx_fee_currency']


# In[ ]:


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


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, query_name, unique_keys = unique_cols)

