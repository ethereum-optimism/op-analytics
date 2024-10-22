#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# https://dune.com/queries/3641455
# Splits by Category

import pandas as pd

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
sys.path.pop()

period = 'day'
trailing_periods = 365


# In[ ]:


par_pd = d.generate_query_parameter(input= trailing_periods, field_name= 'trailing_periods', dtype= 'number')
par_gran = d.generate_query_parameter(input=period, field_name= 'time_granularity', dtype= 'text')

df = d.get_dune_data(3641455, 'l2_transaction_qualification',path='outputs'
                        , params = [par_pd,par_gran]
                        , performance = 'large', num_hours_to_rerun=0
                        )


# In[ ]:




