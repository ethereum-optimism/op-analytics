#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('get l1 costs')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import google_bq_utils as bqu
sys.path.pop()

import time
import dotenv


from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

import numpy as np
import pandas as pd
import os


# In[ ]:


trailing_days = 180 #Assume we always catch chains within 6 months (adjust as needed)
chain_layers = ['ethereum','base']


# In[ ]:


# Run Dune
print('     dune runs')
dune_dfs=[]
dune = DuneClient(os.environ["DUNE_API_KEY"])

days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')
for chain in chain_layers:
    chain_param = d.generate_query_parameter(input=chain,field_name='blockchain',dtype='text')

    int_df = d.get_dune_data(query_id = 3912454, #https://dune.com/queries/3912454
        name = f"daily_op_stack_chains_l1_data_{chain}",
        path = "outputs",
        performance="large",
        params = [days_param, chain_param],
        num_hours_to_rerun=0
    )
    dune_dfs.append(int_df)

non_empty_dfs = [df for df in dune_dfs if not df.empty]
dune_df = pd.concat(non_empty_dfs) if non_empty_dfs else pd.DataFrame()

dune_df['source'] = 'dune'


# In[ ]:


dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)
dune_df['avg_l1_calldata_gas_price_on_l1_inbox'] = dune_df['avg_l1_calldata_gas_price_on_l1_inbox'].astype(float)
dune_df['calldata_bytes_l1_inbox'] = dune_df['calldata_bytes_l1_inbox'].fillna(0).astype(int)
dune_df['l1_gas_used_combined'] = dune_df['l1_gas_used_combined'].fillna(0).astype(int)
dune_df['l1_gas_used_inbox'] = dune_df['l1_gas_used_inbox'].fillna(0).astype(int)
dune_df['num_l1_submissions'] = dune_df['num_l1_submissions'].fillna(0).astype(int)
dune_df['num_l1_txs_combined'] = dune_df['num_l1_txs_combined'].fillna(0).astype(int)
dune_df['num_l1_txs_inbox'] = dune_df['num_l1_txs_inbox'].fillna(0).astype(int)


# In[ ]:


# Fill NULL values in unique key columns
unique_keys_list = ['chain_id', 'dt', 'chain_version','output_root_data_source', 'data_availability_data_source']

for key in unique_keys_list:
    dune_df[key] = dune_df[key].fillna('none')


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, 'daily_op_stack_chains_l1_data', unique_keys = unique_keys_list)

