#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# query: https://dune.com/queries/
query_id = 3445473
query_name = 'dune_evms_info'


# In[ ]:


import sys
import pandas as pd
sys.path.append("../helper_functions")
import duneapi_utils as d
import google_bq_utils as bqu
sys.path.pop()


# In[ ]:


dune_df = d.get_dune_data(query_id = query_id,
    name = query_name,
    path = "outputs"
)


# In[ ]:


bqu.write_df_to_bq_table(dune_df, query_name)

