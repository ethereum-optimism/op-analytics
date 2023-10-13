#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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


# In[ ]:


# Map Chain Names
chain_mappings = {
    'zora': 'Zora Network',
    'pgn': 'Public Goods Network',
    'base': 'Base Mainnet'
    # Add more mappings as needed
}


# In[ ]:


# Get Session ID

session_id = mb.get_session_id(mb_url_base, mb_name, mb_pw)

# try: #do you already have a session
#         session_id = os.environ["MS_METABASE_SESSION_ID"]
#         # Test if session ID
#         mb.get_mb_query_response(mb_url_base, session_id, 42, num_retries = 1)
# except: #if not, make one
#         print('creating new session')
#         session_id = mb.get_mb_session_key(mb_url_base,mb_name,mb_pw)


# In[ ]:


# print(session_id)


# In[ ]:


# Run Query

#https://dash.goldsky.com/question/20-get-kr1-active-developers
query_num = 20
print(query_num)
resp = mb.get_mb_query_response(mb_url_base, session_id, query_num, num_retries = 3)
# print(resp)


# In[ ]:


try:
    data_df = pd.DataFrame(resp)
except ValueError as e:
    print(f"Error in creating DataFrame: {e}")

print("Type of response:", type(resp))

if resp:
    print("First element of the list:", resp[0])
else:
    print("The list is empty")

keys_set = {frozenset(d.keys()) for d in resp if isinstance(d, dict)}
if len(keys_set) > 1:
    print("Dictionaries have different sets of keys.")
else:
    print("All dictionaries have the same set of keys.")

standard_keys = set(resp[0].keys())

for i, dic in enumerate(resp):
    # Get the set of keys of the current dictionary
    current_keys = set(dic.keys())
    
    # Check if the current set of keys matches the standard set of keys
    if current_keys != standard_keys:
        print(f"Dictionary at index {i} does not have the standard set of keys.")
        print(f"Dictionary keys: {current_keys}")
        print(f"Standard keys:   {standard_keys}")
        print(f"Dictionary content: {dic}")


# In[ ]:


data_df['chain'] = data_df['chain'].replace(chain_mappings)

print(data_df.columns)

print(data_df.sample(5))


# In[ ]:


# Post to Dune API
d.write_dune_api_from_pandas(data_df, 'opchain_active_dev_creators_gs',\
                             'Basic Active Developer Methodology for Zora & PGN (from Goldsky)')

