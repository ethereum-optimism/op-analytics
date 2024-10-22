#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from IPython.display import display  # So that display is recognized in .py files

sys.path.append("../helper_functions")
import os
import duneapi_utils as d


# In[ ]:


# add any data to pull here
data = {
    1886707: "dune_op_distribution_type",
    2199844: "dune_usage_by_app",
    2195796: "dune_op_program_performance_summary",
}


# In[ ]:


for query_id, csv_name in data.items():
    d.get_dune_data(query_id, csv_name)

