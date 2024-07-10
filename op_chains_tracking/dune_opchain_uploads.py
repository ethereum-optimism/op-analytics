#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests as r
import pandas as pd
import numpy as np
import os

import sys
sys.path.append("../helper_functions")
import duneapi_utils as du
import pandas_utils as p
import google_bq_utils as bqu
sys.path.pop()

import time


# In[ ]:


table_name = 'daily_opchain_aggregate_stats_dune'

trailing_days = 90
min_txs_per_day = 0


# In[ ]:


# Run Dune
print('     dune runs')
par_pd = du.generate_query_parameter(input= trailing_days, field_name= 'Trailing Num Days', dtype= 'number')
txs_pd = du.generate_query_parameter(input= min_txs_per_day, field_name= 'Min Transactions per Day', dtype= 'number')

dune_df = du.get_dune_data(query_id = 2453515, #https://dune.com/queries/2453515
    name = "dune_" + table_name,
    path = "outputs",
    # performance="large",
    params = [par_pd,txs_pd],
    num_hours_to_rerun = 0, #always rerun because of param
)
dune_df['source'] = 'dune'
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)


# In[ ]:


# Replace inf, -inf, and NaN with 0
dune_df = dune_df.replace([np.inf, -np.inf, np.nan], 0)
dune_df = dune_df.replace(['inf', 'NaN'], 0)


# In[ ]:


dune_df[['dt','name']].head(10)


# In[ ]:


dune_df['dt_by_day'] = pd.to_datetime(dune_df['dt_by_day'])

# List of words to check in column names
keywords = ['_ratio', 'pct_', 'per_tx', 'per_100k_txs', 'avg_']
# Cast columns to float if their name contains any of the keywords
for col in dune_df.columns:
    if any(keyword in col for keyword in keywords):
        try:
            dune_df[col] = dune_df[col].astype('float64')
        except:
            print('error: ' + col)
            continue

columns_to_string = [
    'source','chain_layer','chain_type'
]
# Cast each column in the list to int64
for col in columns_to_string:
    dune_df[col] = dune_df[col].astype('string')

# List of columns to cast to int64
columns_to_int64 = [
    'active_secs_per_day',
    'l1_gas_used_on_l2','l1_gas_used_user_txs_l2_per_day',
    'l2_gas_used','l2_gas_used_user_txs_per_day','l2_gas_used_by_day',
    'total_available_l2_gas_target','calldata_bytes_l2_per_day'
]
# Cast each column in the list to int64
for col in columns_to_int64:
    dune_df[col] = dune_df[col].fillna(0).astype('int64')

# List of columns to cast to float64
columns_to_float64 = [
    'avg_blob_base_fee_pct_margin','avg_l1_gas_price_on_l2_by_day',
    'avg_blob_base_fee_pct_margin_by_day','avg_blob_gas_price_on_l2_by_day',
    'avg_l1_gas_price_fee_pct_margin','avg_l1_gas_price_fee_pct_margin_by_day'

]
# Cast each column in the list to int64
for col in columns_to_float64:
    dune_df[col] = dune_df[col].astype('float64')

# # dune_df.dtypes
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     # display(dune_df.sample(5))
#     print("\nData types:")
#     print(dune_df.dtypes[40:])

# print(dune_df["chain_layer"].unique())
# print(dune_df["chain_layer"].dtypes)


# In[ ]:


dune_df['calldata_bytes_l2_per_day'].dtypes


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, table_name, unique_keys = ['dt','name'])


# In[ ]:


# bqu.delete_bq_table('api_table_uploads',table_name)

