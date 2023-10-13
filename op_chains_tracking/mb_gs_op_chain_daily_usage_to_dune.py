#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests as r
import pandas as pd

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import metabaseapi_utils as mb
sys.path.pop()

import os
import dotenv

dotenv.load_dotenv()
mb_name = os.environ["MS_METABASE_NAME"]
mb_pw = os.environ["MS_METABASE_PW"]

mb_url_base = "https://dash.goldsky.com"

# https://goldsky.notion.site/SHARED-Lightweight-API-Documentation-for-Goldsky-Dashboarding-5cde15ba222844f485c31a4426f6ed53


# In[2]:


# Map Chain Names
chain_mappings = {
    'zora': 'Zora Network',
    'pgn': 'Public Goods Network',
    # Add more mappings as needed
}


# In[3]:


session_id = mb.get_session_id(mb_url_base, mb_name, mb_pw)

# try: #do you already have a session
#         session_id = os.environ["MS_METABASE_SESSION_ID"]
# except: #if not, make one
#         print('creating new session')
#         session_id = mb.get_mb_session_key(mb_url_base,mb_name,mb_pw)


# In[4]:


if (os.environ["IS_RUNNING_LOCAL"]):
        print(session_id)


# In[5]:


# Run Query

#21-op-chains-activity-by-day
query_num = 21
print(query_num)
resp = mb.get_mb_query_response(mb_url_base, session_id, query_num, num_retries = 3)
# print(resp)


# In[6]:


data_df = pd.DataFrame(resp)

data_df['chain'] = data_df['chain'].replace(chain_mappings)
# data_df['dt'] = pd.to_datetime(data_df['dt'])

print(data_df.columns)
# print(data_df.dtypes)

data_df = data_df.sort_values(by='dt',ascending=False)

print(data_df.head(5))


# In[7]:


# Post to Dune API
d.write_dune_api_from_pandas(data_df, 'opchain_activity_by_day_gs',\
                             'Basic Daily Activity for OP Chains - Zora & PGN (from Goldsky)')

