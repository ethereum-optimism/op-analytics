#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start l2 activity')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import growthepieapi_utils as gtp
import l2beat_utils as ltwo
sys.path.pop()

import numpy as np
import pandas as pd


# In[ ]:


# # # Usage
gtp_api = gtp.get_growthepie_api_data()
# # print(merged_df.sample(5))
gtp_meta_api = gtp.get_growthepie_api_meta()


# In[ ]:


l2beat_df = ltwo.get_all_l2beat_data()
l2beat_meta = ltwo.get_l2beat_metadata()
l2beat_meta['chain'] = l2beat_meta['slug']


# In[ ]:


# print(l2beat_df[l2beat_df['chain']=='degen'].sample(5))
# print(l2beat_meta[l2beat_meta['layer']=='layer2'].sample(5))
# print(l2beat_meta[l2beat_meta['layer']=='layer3'].sample(5))


# In[ ]:


# display(l2beat_meta.sample(5))


# In[ ]:


combined_l2b_df = l2beat_df.merge(l2beat_meta[['chain','name','layer','chainId','provider','category','is_upcoming']], on='chain',how='left')
# combined_l2b_df.tail(5)


# In[ ]:


combined_gtp_df = gtp_api.merge(gtp_meta_api[['origin_key','chain_name']], on='origin_key',how='left')
# combined_gtp_df.sample(5)


# In[ ]:


# Check Columns
# Assuming combined_gtp_df is your DataFrame
column_names = combined_gtp_df.columns

for col in column_names:
    if col.endswith('_usd'):
        # Construct the new column name by replacing '_usd' with '_eth'
        new_col_name = col.replace('_usd', '_eth')
        
        # Check if the new column name exists in the DataFrame
        if new_col_name not in combined_gtp_df.columns:
            # If it doesn't exist, create the column and fill it with nan values
            combined_gtp_df[new_col_name] = np.nan


# In[ ]:


# print(combined_gtp_df.dtypes)
# print(l2beat_df.dtypes)
# combined_gtp_df.sample(5)


# In[ ]:


# export
folder = 'outputs/'
combined_gtp_df.to_csv(folder + 'growthepie_l2_activity.csv', index = False)
gtp_meta_api.to_csv(folder + 'growthepie_l2_metadata.csv', index = False)
combined_l2b_df.to_csv(folder + 'l2beat_l2_activity.csv', index = False)
l2beat_meta.to_csv(folder + 'l2beat_l2_metadata.csv', index = False)
# Post to Dune API
d.write_dune_api_from_pandas(combined_gtp_df, 'growthepie_l2_activity',\
                             'L2 Usage Activity from GrowThePie')
d.write_dune_api_from_pandas(gtp_meta_api, 'growthepie_l2_metadata',\
                             'L2 Metadata from GrowThePie')
d.write_dune_api_from_pandas(combined_l2b_df, 'l2beat_l2_activity',\
                             'L2 Usage Activity from L2Beat')
d.write_dune_api_from_pandas(l2beat_meta, 'l2beat_l2_metadata',\
                             'L2 Metadata from L2Beat')

