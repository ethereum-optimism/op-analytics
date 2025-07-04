#!/usr/bin/env python
# coding: utf-8

# In[1]:


print("start l2 activity")
import sys

sys.path.append("../helper_functions")
import duneapi_utils as d
import growthepieapi_utils as gtp
import l2beat_utils as ltwo
import csv_utils as cu
import google_bq_utils as bqu
import pandas_utils as pu
import clickhouse_utils as ch

sys.path.pop()

import numpy as np
import pandas as pd
import time
from datetime import datetime, timezone


# In[2]:


# L2B Meta
l2b_summary = ltwo.get_l2beat_chain_summary()
l2b_summary['dt'] = pd.to_datetime(datetime.now(timezone.utc).date())


# In[3]:


# display(l2b_summary.head(5))
# print(l2b_summary.dtypes)

# bqu.append_and_upsert_df_to_bq_table(
#     l2b_summary,
#     "daily_l2beat_chain_summary",
#     unique_keys=["dt", "id","slug"],
# )


# In[4]:


# # # Usage
try:
    gtp_api = gtp.get_growthepie_api_data()
    gtp_meta_api = gtp.get_growthepie_api_meta()
    gtp_api = gtp_api.rename(columns={"date": "dt"})
    
    # update datatype for bq uploads
    if "enable_contracts" in gtp_meta_api:
        gtp_meta_api["enable_contracts"] = gtp_meta_api["enable_contracts"].astype(bool)

    if "colors" in gtp_meta_api:
        gtp_meta_api["colors"] = gtp_meta_api["colors"].astype(str)
        
except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    print("Traceback:")
    print(traceback.format_exc())
    # Initialize empty DataFrames if API calls fail
    gtp_api = pd.DataFrame()
    gtp_meta_api = pd.DataFrame()


# In[6]:


print("getting assets onchain - l2beat")
# l2beat_aoc = ltwo.get_daily_aoc_by_token()
# l2beat_aoc = ltwo.aoc.rename(columns={"project": "chain", "date": "dt"})
time.sleep(3)  # rate limits


# In[7]:


print("getting data - l2beat")
# l2beat_df = ltwo.get_all_l2beat_data()


# In[8]:


print("getting metadata - l2beat")
l2beat_meta = ltwo.get_l2beat_metadata()
l2beat_meta["chain"] = l2beat_meta["slug"]

# Ensure all required boolean columns exist
boolean_columns = ["is_upcoming", "is_archived", "is_current_chain"]
for col in boolean_columns:
    if col not in l2beat_meta.columns:
        l2beat_meta[col] = False
    else:
        l2beat_meta[col] = l2beat_meta[col].fillna(False)


# In[9]:


# l2beat_meta[l2beat_meta['chainId'] == '324']
# l2beat_meta


# In[10]:


# combined_l2b_df = l2beat_df.merge(
#     l2beat_meta[
#         [
#             "chain",
#             "name",
#             "layer",
#             "chainId",
#             "provider",
#             "provider_entity",
#             "category",
#             "is_upcoming",
#             "is_archived",
#             "is_current_chain",
#         ]
#     ],
#     on="chain",
#     how="outer",
# )

# combined_l2b_df["chainId"] = combined_l2b_df["chainId"].astype("Int64")


# In[ ]:


print("getting data - growthepie")
try:
    if not gtp_api.empty and not gtp_meta_api.empty:
        combined_gtp_df = gtp_api.merge(
            gtp_meta_api[["origin_key", "chain_name", "evm_chain_id"]],
            on="origin_key",
            how="left",
        )
        combined_gtp_df["dt"] = pd.to_datetime(combined_gtp_df["dt"], errors="coerce")

        combined_gtp_df = combined_gtp_df.drop(columns=("index"))
        # combined_gtp_df.sample(5)
    else:
        print("GrowThePie API data is empty, skipping merge")
        combined_gtp_df = pd.DataFrame()
except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    print("Traceback:")
    print(traceback.format_exc())
    combined_gtp_df = pd.DataFrame()


# In[12]:


# Check Columns
# Assuming combined_gtp_df is your DataFrame
try:
    if not combined_gtp_df.empty:
        column_names = combined_gtp_df.columns

        for col in column_names:
            if col.endswith("_usd"):
                # Construct the new column name by replacing '_usd' with '_eth'
                new_col_name = col.replace("_usd", "_eth")

                # Check if the new column name exists in the DataFrame
                if new_col_name not in combined_gtp_df.columns:
                    # If it doesn't exist, create the column and fill it with nan values
                    combined_gtp_df[new_col_name] = np.nan
except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    print("Traceback:")
    print(traceback.format_exc())


# In[13]:


# Add Metadata
opstack_metadata = pd.read_csv(
    "../op_chains_tracking/outputs/chain_metadata.csv"
)
# combined_l2b_df["l2beat_slug"] = combined_l2b_df["chain"]
# meta_cols = [
#     "l2beat_slug",
#     "is_op_chain",
#     "mainnet_chain_id",
#     "op_based_version",
#     "alignment",
#     "chain_name",
#     "display_name",
# ]

# l2b_enriched_df = combined_l2b_df.merge(
#     opstack_metadata[meta_cols], on="l2beat_slug", how="left"
# )

# l2b_enriched_df["alignment"] = l2b_enriched_df["alignment"].fillna("Other EVMs")


# In[ ]:


boolean_columns = ["is_op_chain", "is_upcoming", "is_archived", "is_current_chain"]
dfs = [l2beat_meta]#[l2b_enriched_df, l2beat_meta]

for df in dfs:
    for column in boolean_columns:
        if column in df.columns:
            df[column] = df[column].fillna(False)


# In[15]:


#  Define aggregation functions for each column
aggregations = {
    "totalUsd": ["min", "last", "mean"],  # valueUsd
    "transactions": ["sum", "mean"],
    "canonicalUsd": ["min", "last", "mean"],  # cbvUsd
    "externalUsd": ["min", "last", "mean"],  # ebvUsd
    "nativeUsd": ["min", "last", "mean"],  # nmvUsd
}


# Function to perform aggregation based on frequency
def aggregate_data(df, freq, date_col="timestamp", groupby_cols=None, aggs=None):
    if groupby_cols is None:
        groupby_cols = [
            "chain",
            "chainId",
            "layer",
            "is_op_chain",
            "mainnet_chain_id",
            "op_based_version",
            "alignment",
            "chain_name",
            "display_name",
            "provider",
            "is_upcoming",
            "is_archived",
            "is_current_chain",
        ]
    if aggs is None:
        aggs = aggregations

    # Group by the specified frequency and other columns, then apply aggregations
    df_agg = (
        df.groupby([pd.Grouper(key=date_col, freq=freq)] + groupby_cols, dropna=False)
        .agg(aggs)
        .reset_index()
    )

    # Flatten the hierarchical column index and concatenate aggregation function names with column names
    df_agg.columns = ["_".join(filter(None, col)).rstrip("_") for col in df_agg.columns]

    # Rename the 'timestamp' column based on the frequency
    date_col_name = "month" if freq == "MS" else "week"
    df_agg.rename(columns={date_col: date_col_name}, inplace=True)

    # Group by 'chain' and rank the rows within each group based on the date column
    df_agg[f"{date_col_name}s_live"] = df_agg.groupby("chain")[date_col_name].rank(
        method="min"
    )

    return df_agg


# # Perform monthly aggregation
# l2b_monthly_df = aggregate_data(l2b_enriched_df, freq="MS")
# # Perform weekly aggregation
# l2b_weekly_df = aggregate_data(l2b_enriched_df, freq="W-MON")

# Sample output
# l2b_weekly_df.sample(5)


# In[16]:


# export
folder = "outputs/"
# combined_gtp_df.to_csv(folder + "growthepie_l2_activity.csv", index=False)
# gtp_meta_api.to_csv(folder + "growthepie_l2_metadata.csv", index=False)
# l2b_enriched_df.to_csv(folder + "l2beat_l2_activity.csv", index=False)
# l2beat_meta.to_csv(folder + "l2beat_l2_metadata.csv", index=False)
# l2b_monthly_df.to_csv(folder + "l2beat_l2_activity_monthly.csv", index=False)
# l2b_weekly_df.to_csv(folder + "l2beat_l2_activity_weekly.csv", index=False)
# l2beat_aoc.to_csv(folder + "l2beat_aoc_by_token.csv", index=False)


# In[17]:


# # gtp_meta_api.dtypes
# gtp_meta_api['logo'] = gtp_meta_api['logo'].astype(str)
# gtp_meta_api['l2beat_stage'] = gtp_meta_api['l2beat_stage'].astype(str)
# # gtp_meta_api['stack'] = gtp_meta_api['stack'].astype(str)
# gtp_meta_api['block_explorers'] = gtp_meta_api['block_explorers'].astype(str)


# In[18]:


# gtp_meta_api


# In[19]:


# combined_gtp_df['evm_chain_id'] = combined_gtp_df['evm_chain_id'].astype(str).replace('.0','').fillna('-')
# gtp_meta_api['evm_chain_id'] = gtp_meta_api['evm_chain_id'].astype(str).replace('.0','').fillna('-')


# In[ ]:


# BQ Upload
bqu.write_df_to_bq_table(l2beat_meta, "l2beat_l2_metadata")
# time.sleep(1)
try:
    if not combined_gtp_df.empty:
        bqu.write_df_to_bq_table(combined_gtp_df, "daily_growthepie_l2_activity")
        time.sleep(1)
    if not gtp_meta_api.empty:
        bqu.write_df_to_bq_table(gtp_meta_api, "growthepie_l2_metadata")
        time.sleep(1)
# bqu.write_df_to_bq_table(l2b_enriched_df, "daily_l2beat_l2_activity")
# time.sleep(1)
# bqu.write_df_to_bq_table(l2b_monthly_df, "monthly_l2beat_l2_activity")
# time.sleep(1)
# bqu.write_df_to_bq_table(l2b_weekly_df, "weekly_l2beat_l2_activity")
# time.sleep(1)
# bqu.append_and_upsert_df_to_bq_table(
#     l2beat_aoc,
#     "daily_l2beat_aoc_by_token",
#     unique_keys=["dt", "chain", "token_type", "asset_id", "chain", "address"],
except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    print("Traceback:")
    print(traceback.format_exc())
# )


# In[ ]:


# Post to Dune API
try:
    if not combined_gtp_df.empty:
        d.write_dune_api_from_pandas(
            combined_gtp_df, "growthepie_l2_activity", "L2 Usage Activity from GrowThePie"
        )
    if not gtp_meta_api.empty:
        d.write_dune_api_from_pandas(
            gtp_meta_api, "growthepie_l2_metadata", "L2 Metadata from GrowThePie"
        )
except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    print("Traceback:")
    print(traceback.format_exc())
# d.write_dune_api_from_pandas(
#     l2b_enriched_df, "l2beat_l2_activity", "L2 Usage Activity from L2Beat"
# )
# d.write_dune_api_from_pandas(
#     l2b_monthly_df,
#     "l2beat_l2_activity_monthly",
#     "Monthly L2 Usage Activity from L2Beat",
# )
# d.write_dune_api_from_pandas(
#     l2b_weekly_df, "l2beat_l2_activity_weekly", "Weekly L2 Usage Activity from L2Beat"
# )
# d.write_dune_api_from_pandas(
#     l2beat_meta, "l2beat_l2_metadata", "L2 Metadata from L2Beat"
# )


# In[ ]:


# Copy to Dune
print('upload bq to dune')
sql = '''
SELECT *
FROM `views.daily_l2beat_breakdown`
WHERE onchain_value_usd > 0
AND dt_day >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
'''
bq_df = bqu.run_query_to_df(sql)

dune_table_name = 'daily_l2beat_breakdown'
d.write_dune_api_from_pandas(bq_df, dune_table_name,table_description = dune_table_name)

