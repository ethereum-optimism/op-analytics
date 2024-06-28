#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start l2 activity')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import growthepieapi_utils as gtp
import l2beat_utils as ltwo
import csv_utils as cu
import google_bq_utils as bqu
import pandas_utils as pu
sys.path.pop()

import numpy as np
import pandas as pd
import time


# In[ ]:


# # # Usage
gtp_api = gtp.get_growthepie_api_data()
gtp_meta_api = gtp.get_growthepie_api_meta()
gtp_api = gtp_api.rename(columns={'date':'dt'})


# In[ ]:


# gtp_meta_api.sample(10)
# gtp_meta_api.dtypes


# In[ ]:


l2beat_aoc = ltwo.get_daily_aoc_by_token()
l2beat_aoc = l2beat_aoc.rename(columns={'project':'chain','date':'dt'})


# In[ ]:


l2beat_df = ltwo.get_all_l2beat_data()


# In[ ]:


l2beat_meta = ltwo.get_l2beat_metadata()
l2beat_meta['chain'] = l2beat_meta['slug']
l2beat_meta['is_upcoming'] = l2beat_meta['is_upcoming'].fillna(False)


# In[ ]:


combined_l2b_df = l2beat_df.merge(l2beat_meta[
        ['chain','name','layer','chainId','provider','provider_entity','category',\
         'is_upcoming','is_archived','is_current_chain']
        ], on='chain',how='outer')

combined_l2b_df['chainId'] = combined_l2b_df['chainId'].astype('Int64')


# In[ ]:


combined_gtp_df = gtp_api.merge(gtp_meta_api[['origin_key','chain_name']], on='origin_key',how='left')
combined_gtp_df["dt"] = pd.to_datetime(combined_gtp_df["dt"], errors='coerce')

combined_gtp_df = combined_gtp_df.drop(columns=('index'))
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


# Add Metadata
opstack_metadata = opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')
combined_l2b_df['l2beat_slug'] = combined_l2b_df['chain']
meta_cols = ['l2beat_slug', 'is_op_chain','mainnet_chain_id','op_based_version', 'alignment','chain_name', 'display_name']

l2b_enriched_df = combined_l2b_df.merge(opstack_metadata[meta_cols], on='l2beat_slug', how = 'left')

l2b_enriched_df['alignment'] = l2b_enriched_df['alignment'].fillna('Other EVMs')


# In[ ]:


boolean_columns = ['is_op_chain', 'is_upcoming', 'is_archived', 'is_current_chain']
dfs = [l2b_enriched_df, l2beat_meta]

for df in dfs:
    for column in boolean_columns:
        if column in df.columns:
            df[column] = df[column].fillna(False)


# In[ ]:


#  Define aggregation functions for each column
aggregations = {
    'totalUsd': ['min', 'last', 'mean'], #valueUsd
    'transactions': ['sum', 'mean'],
    'canonicalUsd': ['min', 'last', 'mean'], #cbvUsd
    'externalUsd': ['min', 'last', 'mean'], #ebvUsd
    'nativeUsd': ['min', 'last', 'mean'], #nmvUsd
}
# Function to perform aggregation based on frequency
def aggregate_data(df, freq, date_col='timestamp', groupby_cols=None, aggs=None):
    if groupby_cols is None:
        groupby_cols = ['chain', 'chainId', 'layer', 'is_op_chain', 'mainnet_chain_id', 'op_based_version', 'alignment', 'chain_name', 'display_name', 'provider', 'is_upcoming','is_archived','is_current_chain']
    if aggs is None:
        aggs = aggregations

    # Group by the specified frequency and other columns, then apply aggregations
    df_agg = df.groupby([pd.Grouper(key=date_col, freq=freq)] + groupby_cols, dropna=False).agg(aggs).reset_index()

    # Flatten the hierarchical column index and concatenate aggregation function names with column names
    df_agg.columns = ['_'.join(filter(None, col)).rstrip('_') for col in df_agg.columns]

    # Rename the 'timestamp' column based on the frequency
    date_col_name = 'month' if freq == 'MS' else 'week'
    df_agg.rename(columns={date_col: date_col_name}, inplace=True)

    # Group by 'chain' and rank the rows within each group based on the date column
    df_agg[f'{date_col_name}s_live'] = df_agg.groupby('chain')[date_col_name].rank(method='min')

    return df_agg

# Perform monthly aggregation
l2b_monthly_df = aggregate_data(l2b_enriched_df, freq='MS')
# Perform weekly aggregation
l2b_weekly_df = aggregate_data(l2b_enriched_df, freq='W-MON')

# Sample output
# l2b_weekly_df.sample(5)


# In[ ]:


# export
folder = 'outputs/'
combined_gtp_df.to_csv(folder + 'growthepie_l2_activity.csv', index = False)
gtp_meta_api.to_csv(folder + 'growthepie_l2_metadata.csv', index = False)
l2b_enriched_df.to_csv(folder + 'l2beat_l2_activity.csv', index = False)
l2beat_meta.to_csv(folder + 'l2beat_l2_metadata.csv', index = False)
l2b_monthly_df.to_csv(folder + 'l2beat_l2_activity_monthly.csv', index = False)
l2b_weekly_df.to_csv(folder + 'l2beat_l2_activity_weekly.csv', index = False)
l2beat_aoc.to_csv(folder + 'l2beat_aoc_by_token.csv', index = False)
# Post to Dune API
d.write_dune_api_from_pandas(combined_gtp_df, 'growthepie_l2_activity',\
                             'L2 Usage Activity from GrowThePie')
d.write_dune_api_from_pandas(gtp_meta_api, 'growthepie_l2_metadata',\
                             'L2 Metadata from GrowThePie')
d.write_dune_api_from_pandas(l2b_enriched_df, 'l2beat_l2_activity',\
                             'L2 Usage Activity from L2Beat')
d.write_dune_api_from_pandas(l2b_monthly_df, 'l2beat_l2_activity_monthly',\
                             'Monthly L2 Usage Activity from L2Beat')
d.write_dune_api_from_pandas(l2b_weekly_df, 'l2beat_l2_activity_weekly',\
                             'Weekly L2 Usage Activity from L2Beat')
d.write_dune_api_from_pandas(l2beat_meta, 'l2beat_l2_metadata',\
                             'L2 Metadata from L2Beat')


# In[ ]:


#BQ Upload
bqu.write_df_to_bq_table(combined_gtp_df, 'daily_growthepie_l2_activity')
time.sleep(1)
bqu.write_df_to_bq_table(gtp_meta_api, 'growthepie_l2_metadata')
time.sleep(1)
bqu.write_df_to_bq_table(l2b_enriched_df, 'daily_l2beat_l2_activity')
time.sleep(1)
bqu.write_df_to_bq_table(l2b_monthly_df, 'monthly_l2beat_l2_activity')
time.sleep(1)
bqu.write_df_to_bq_table(l2b_weekly_df, 'weekly_l2beat_l2_activity')
time.sleep(1)
bqu.write_df_to_bq_table(l2beat_meta, 'l2beat_l2_metadata')
time.sleep(1)
bqu.append_and_upsert_df_to_bq_table(l2beat_aoc, 'daily_l2beat_aoc_by_token', unique_keys=['dt','chain','token_type','asset_id','chain','address'])

