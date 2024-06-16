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

trailing_days = 365


# In[ ]:


# Run Dune
print('     dune runs')
par_pd = du.generate_query_parameter(input= trailing_days, field_name= 'Trailing Num Days', dtype= 'number')

dune_df = du.get_dune_data(query_id = 2453515, #https://dune.com/queries/2453515
    name = "dune_" + table_name,
    path = "outputs",
    # performance="large",
    params = [par_pd],
    num_hours_to_rerun = 0, #always rerun because of param
)
dune_df['source'] = 'dune'
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)


# In[ ]:


# Replace inf, -inf, and NaN with 0
dune_df = dune_df.replace([np.inf, -np.inf, np.nan], 0)
dune_df = dune_df.replace(['inf', 'NaN'], 0)


# In[ ]:


# dune_df.dtypes


# In[ ]:


dune_df['dt_by_day'] = pd.to_datetime(dune_df['dt_by_day'])
dune_df["avg_l1_calldata_gas_price_on_l1_inbox"] = dune_df["avg_l1_calldata_gas_price_on_l1_inbox"].astype('float64')
dune_df["avg_l1_calldata_gas_price_on_l1_inbox_by_day"] = dune_df["avg_l1_calldata_gas_price_on_l1_inbox_by_day"].astype('float64')
dune_df["source"] = dune_df["source"].astype('string')
dune_df["chain_layer"] = dune_df["chain_layer"].astype('string')
dune_df["chain_type"] = dune_df["chain_type"].astype('string')

# # dune_df.dtypes
# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     # display(dune_df.sample(5))
#     print("\nData types:")
#     print(dune_df.dtypes[40:])

# print(dune_df["chain_layer"].unique())
# print(dune_df["chain_layer"].dtypes)


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, table_name, unique_keys = ['dt','name'])


# In[ ]:


# bqu.delete_bq_table('api_table_uploads',table_name)

