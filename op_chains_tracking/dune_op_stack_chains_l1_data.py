#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('get qualified txs')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import google_bq_utils as bqu
sys.path.pop()

import time

import numpy as np
import pandas as pd
import os


# In[ ]:


trailing_days = 180 #Assume we always catch chains within 6 months


# In[ ]:


# Run Dune
print('     dune runs')
days_param = d.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')
dune_df = d.get_dune_data(query_id = 3912454, #https://dune.com/queries/3912454
    name = "daily_op_stack_chains_l1_data",
    path = "outputs",
    performance="large",
    params = [days_param],
    num_hours_to_rerun=0 #always rereun
)
dune_df['source'] = 'dune'
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)


# In[ ]:


dune_df.dtypes
dune_df['avg_l1_calldata_gas_price_on_l1_inbox'] = dune_df['avg_l1_calldata_gas_price_on_l1_inbox'].astype(float)


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, 'daily_op_stack_chains_l1_data', unique_keys = ['chain_id','dt','name'])

