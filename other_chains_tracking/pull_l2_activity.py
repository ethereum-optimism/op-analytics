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


l2beat_meta[l2beat_meta['slug'].str.contains('zksync')]


# In[ ]:


# print(l2beat_df[l2beat_df['chain']=='degen'].sample(5))
# print(l2beat_meta[l2beat_meta['layer']=='layer2'].sample(5))
# print(l2beat_meta[l2beat_meta['layer']=='layer3'].sample(5))


# In[ ]:


combined_l2b_df = l2beat_df.merge(l2beat_meta[['chain','name','layer','chainId','provider','provider_entity','category','is_upcoming']], on='chain',how='outer')
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
combined_l2b_df[combined_l2b_df['chain']=='zksync'].sample(5)


# In[ ]:


# Add Metadata
opstack_metadata = opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')
combined_l2b_df['l2beat_slug'] = combined_l2b_df['chain']
meta_cols = ['l2beat_slug', 'is_op_chain','mainnet_chain_id','op_based_version', 'alignment','chain_name', 'display_name']

l2b_enriched_df = combined_l2b_df.merge(opstack_metadata[meta_cols], on='l2beat_slug', how = 'left')

l2b_enriched_df['alignment'] = l2b_enriched_df['alignment'].fillna('Other EVMs')
l2b_enriched_df['is_op_chain'] = l2b_enriched_df['is_op_chain'].fillna(False)


# In[ ]:


#  Define aggregation functions for each column
aggregations = {
    'valueUsd': ['min', 'last', 'mean'],
    'transactions': ['sum', 'mean'],
    'cbvUsd': ['min', 'last', 'mean'],
    'ebvUsd': ['min', 'last', 'mean'],
    'nmvUsd': ['min', 'last', 'mean'],
}

# Group by month, chain, layer, and other specified columns and apply aggregations
l2b_monthly_df = l2b_enriched_df.groupby([pd.Grouper(key='timestamp', freq='MS'), 'chain', 'layer', 'is_op_chain', 'mainnet_chain_id', 'op_based_version', 'alignment', 'chain_name','display_name','provider','is_upcoming'], dropna=False).agg(aggregations).reset_index()

# Flatten the hierarchical column index and concatenate aggregation function names with column names
l2b_monthly_df.columns = [f'{col}_{func}' if func != '' else col for col, func in l2b_monthly_df.columns]
# Rename the 'date' column
l2b_monthly_df.rename(columns={'timestamp': 'month'}, inplace=True)
# Group by 'chain' and rank the rows within each group based on the 'date' column
l2b_monthly_df['months_live'] = l2b_monthly_df.groupby('chain')['month'].rank(method='min')
l2b_monthly_df.sample(5)


# In[ ]:


# export
folder = 'outputs/'
combined_gtp_df.to_csv(folder + 'growthepie_l2_activity.csv', index = False)
gtp_meta_api.to_csv(folder + 'growthepie_l2_metadata.csv', index = False)
l2b_enriched_df.to_csv(folder + 'l2beat_l2_activity.csv', index = False)
l2beat_meta.to_csv(folder + 'l2beat_l2_metadata.csv', index = False)
l2b_monthly_df.to_csv(folder + 'l2beat_l2_activity_monthly.csv', index = False)
# Post to Dune API
d.write_dune_api_from_pandas(combined_gtp_df, 'growthepie_l2_activity',\
                             'L2 Usage Activity from GrowThePie')
d.write_dune_api_from_pandas(gtp_meta_api, 'growthepie_l2_metadata',\
                             'L2 Metadata from GrowThePie')
d.write_dune_api_from_pandas(l2b_enriched_df, 'l2beat_l2_activity',\
                             'L2 Usage Activity from L2Beat')
d.write_dune_api_from_pandas(l2b_monthly_df, 'l2beat_l2_activity_monthly',\
                             'Monthly L2 Usage Activity from L2Beat')
d.write_dune_api_from_pandas(l2beat_meta, 'l2beat_l2_metadata',\
                             'L2 Metadata from L2Beat')

