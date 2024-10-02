#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start tokenlist uploads')

import requests as r
import pandas as pd
import os

import sys
sys.path.append("../helper_functions")
import github_utils as gh
import tokenlist_utils as tl
import duneapi_utils as d
import clickhouse_utils as ch
import google_bq_utils as bqu
sys.path.pop()

import time


# In[ ]:


table_name = 'superchain_tokenlist'


# In[ ]:


url_content = [
    # owner_name, repo_name, path_name, file_name
      ['ethereum-optimism','ethereum-optimism.github.io','','optimism.tokenlist.json',] #OP Bridge - https://github.com/ethereum-optimism/ethereum-optimism.github.io/blob/master/optimism.tokenlist.json
      # Add other tokenlists if needed
]


# In[ ]:


df_list = []
for gh_url in url_content:
        owner_name, repo_name, path_name, file_name = gh_url

        gh_file = gh.get_file_url_from_github(owner_name, repo_name, path_name, file_name)
        res = r.get(gh_file)
        data = res.json()
    
        if owner_name == 'ethereum-optimism':
                tmp = tl.generate_op_table_from_tokenlist(data)
        else:
                tmp = tl.generate_table_from_tokenlist(data)
                
        tmp['list_name'] = owner_name
        df_list.append(tmp)

df = pd.concat(df_list, ignore_index=True)


# In[ ]:


df.sample(5)


# In[ ]:


# #BQ Upload
bqu.write_df_to_bq_table(df, table_name)

# CH Upload
ch.write_df_to_clickhouse(df, table_name)

# Dune Upload
d.write_dune_api_from_pandas(df, table_name,table_description = table_name)

