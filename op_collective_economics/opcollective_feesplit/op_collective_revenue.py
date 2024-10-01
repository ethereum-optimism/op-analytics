#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests as r
import pandas as pd
import numpy as np
import os

import sys
sys.path.append("../../helper_functions")
import duneapi_utils as du
import pandas_utils as p
import google_bq_utils as bqu
sys.path.pop()

import time


# In[ ]:


table_name = 'op_collective_revenue_transactions'

trailing_days = 365


# In[ ]:


# Run Dune
print('     dune runs')
days_param = du.generate_query_parameter(input=trailing_days,field_name='trailing_days',dtype='number')
dune_df = du.get_dune_data(query_id = 3046107, #https://dune.com/queries/3046107
    name = table_name,
    path = "outputs",
    performance="medium",
    params = [days_param],
    num_hours_to_rerun=4
)
dune_df['source'] = 'dune'
dune_df['tx_block_time'] = pd.to_datetime(dune_df['tx_block_time']).dt.tz_localize(None)


# In[ ]:


dune_df['value'] = dune_df['value'].astype(str)
dune_df['chain_id'] = dune_df['chain_id'].astype(str)


# In[ ]:


#BQ Upload
bqu.append_and_upsert_df_to_bq_table(dune_df, table_name, unique_keys = ['tx_block_number','tx_hash','trace_address'])

