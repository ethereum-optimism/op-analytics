#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests as r
import pandas as pd
import os

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import google_bq_utils as bqu
sys.path.pop()


# In[ ]:


# Copy to Dune
print('upload bq to dune')
sql = '''
SELECT *
FROM `oplabs-tools-data.views.daily_chain_usage_data`
WHERE dt >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 YEAR)
'''
bq_df = bqu.run_query_to_df(sql)

dune_table_name = 'daily_chain_usage_data'
d.write_dune_api_from_pandas(bq_df, dune_table_name,table_description = dune_table_name)

