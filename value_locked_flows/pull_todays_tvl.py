#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sys
import numpy as np
sys.path.append("../helper_functions")
import defillama_utils as dfl
sys.path.pop()


# In[2]:


df = dfl.get_todays_tvl()
df = df[df['chainTVL'] > 0] # Pull > 0 TVL
df = df[~df['chain'].str.contains('-')] # Filter out all pool2, borrows, staking, etc
df['chain_map'] = np.where(df['chain']=='Optimism','optimism','other chains')
df.to_csv('csv_outputs/dfl_data.csv',index=False)

