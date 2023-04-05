#!/usr/bin/env python
# coding: utf-8

# In[1]:


# STILL WIP

# # Read from:
# Dune - OP Deployed by deployer address type
# Defillama/Subgraphs - TVL Flows by Program
# Notion - OP Budget by Program

# Join these datasets together on program & associate anything else to the generalized programs


# In[2]:


import pandas as pd
import numpy as np
import datetime
from IPython.display import display #So that display is recognized in .py files


# In[3]:


tvl = pd.read_csv("csv_outputs/op_summer_latest_stats.csv")
distrib_df = pd.read_csv("csv_outputs/dune_op_distribution_type.csv")
program_df = pd.read_csv("inputs/op_incentive_program_info.csv")
app_df = pd.read_csv("csv_outputs/dune_usage_by_app.csv")


# In[4]:


# Filter TVL DF
tvl = tvl[tvl["include_in_summary"] == 1]
tvl["join_key"] = tvl["top_level_name"].str.replace(
    "*", ""
)  # tvl['app_name'] + ' - ' + tvl['top_level_name'].str.replace('*','')
# display(tvl)


# In[5]:


op_token_columns = [
    "op_claimed",
    "op_deployed",
    "op_from_other_projects",
    "op_to_other_projects",
    "op_to_project",
]


# In[6]:


# Set up Distributions for Mapping
distrib_df["program_map"] = np.where(
    distrib_df["counterparty_type"].isin(tvl["top_level_name"]),
    distrib_df["counterparty_type"],
    "",
)
group_cols = [
    "project_name",
    "counterparty_label",
    "counterparty_type",
    "program_map",
] + op_token_columns

sum_distrib_df = distrib_df[group_cols].groupby(["project_name", "program_map"]).sum()
sum_distrib_df.reset_index(inplace=True)
# Joins should maybe just be the program map OR from name, since Velo operated bribes for a while
sum_distrib_df["join_key"] = np.where(
    sum_distrib_df["program_map"] == "",
    sum_distrib_df["project_name"],
    sum_distrib_df["program_map"],
)
# sum_distrib_df['from_name'] + ' - ' \
#     + np.where(sum_distrib_df['program_map'] == '',sum_distrib_df['from_name'],sum_distrib_df['program_map'])

# display(sum_distrib_df[sum_distrib_df['join_key'].str.contains('elodr')])


# In[7]:


# lowercase joinkeys
tvl["join_key"] = tvl["join_key"].str.lower()
sum_distrib_df["join_key"] = sum_distrib_df["join_key"].str.lower()
#
df = sum_distrib_df.merge(tvl, on="join_key", how="outer")
# display(df[df['join_key'].str.contains('velodr')])


# In[8]:


# Overrides as needed
def replace_program_names(df, overrides):
    for program, program_override in overrides.items():
        df.loc[df["join_key"] == program, "from_name"] = program_override
    return df


# Overrides if needed
overrides = {
    "old name": "new name",
}

# Replace program names with overrides
df = replace_program_names(df, overrides)


# In[9]:


# Create the aggregate app name field
df["agg_app_name"] = df["app_name"].combine_first(df["project_name"])
df = df.fillna(0)  # Fill NA with 0
# display(df)


# In[10]:


# Now union back again

data_cols = [
    "agg_app_name",
    "top_level_name",
    "program_name",
    "num_op_override",
    "period",
    "op_source",
    "start_date",
    "end_date",
    "cumul_net_dollar_flow_at_program_end",
    "cumul_net_dollar_flow",
    "cumul_last_price_net_dollar_flow_at_program_end",
    "cumul_last_price_net_dollar_flow",
]
select_cols = data_cols + op_token_columns

group_cols = select_cols[:8]  # group by 1 to 8
print(group_cols)

# display(df[select_cols])

sum_distrib_df = df[select_cols].groupby(group_cols).sum()
sum_distrib_df.reset_index(inplace=True)


# In[11]:


# Get the rank by start_date of each program

sum_distrib_df = sum_distrib_df.reset_index().rename(columns={"index": "row_num"})

# replace 0s with '9999-12-31'
sum_distrib_df["start_date"] = np.where(
    sum_distrib_df["start_date"] == 0, "9999-12-31", sum_distrib_df["start_date"]
)
# create a new column 'program_rank' based on the 'start_date' column
sum_distrib_df = sum_distrib_df.sort_values(["agg_app_name", "start_date", "row_num"])
sum_distrib_df["program_rank"] = sum_distrib_df.groupby("agg_app_name").cumcount() + 1

sum_distrib_df = sum_distrib_df.sort_values(
    by=["agg_app_name", "program_rank"], ascending=[True, True]
)
# subtract all overridden values fromthe amount I have deployed

# create a new column 'cumulative_num_op_override' that contains the cumulative sum of 'num_op_override' for each agg_app_name group
sum_distrib_df["cumulative_num_op_override"] = sum_distrib_df.groupby("agg_app_name")[
    "num_op_override"
].cumsum()


# create a new column 'op_deployed_net_override' that subtracts 'cumulative_num_op_override' from 'op_deployed'
sum_distrib_df["op_deployed_net_override"] = sum_distrib_df[
    "op_deployed"
] - sum_distrib_df.groupby("agg_app_name")["cumulative_num_op_override"].shift(
    1
).fillna(
    0
)
# drop the 'cumulative_num_op_override' column
# sum_distrib_df.drop('cumulative_num_op_override', axis=1, inplace=True)

# replace '9999-12-31' with 0s
sum_distrib_df["start_date"] = np.where(
    sum_distrib_df["start_date"] == "9999-12-31", 0, sum_distrib_df["start_date"]
)

# Drop Row Num
sum_distrib_df.drop("row_num", axis=1, inplace=True)
# display(sum_distrib_df[sum_distrib_df['agg_app_name'].str.contains('rrakis')])


# In[12]:


# Now do the algorithmic overrides - where we want to redistirbute deployed OP across specific programs (i.e. Uniswap LM w/ Partners)
# # replace 0s in 'num_op_override' with the corresponding value in 'op_deployed_net_override'
sum_distrib_df["og_op_deployed"] = sum_distrib_df["op_deployed"]
# Override # OP Deployed
sum_distrib_df["op_deployed"] = np.where(
    (sum_distrib_df["num_op_override"] == 0),
    sum_distrib_df["op_deployed_net_override"],
    sum_distrib_df["num_op_override"],
)

# Hardcode for Aave - Liquidity Mining since claims came straight from the FND wallet. This should be a one-time edge case
sum_distrib_df["op_deployed"] = np.where(
    sum_distrib_df["top_level_name"] == "Aave - Liquidity Mining",
    5_000_000,
    sum_distrib_df["op_deployed"],
)


# In[13]:


# #Select all except the last 4 rows
sum_distrib_df = sum_distrib_df.iloc[:, :-4]


# In[14]:


# latest tvl metrics
sum_distrib_df["cumul_last_price_net_dollar_flow_at_program_end_ended"] = np.where(
    sum_distrib_df["period"] == "Post-Program",
    sum_distrib_df["cumul_last_price_net_dollar_flow_at_program_end"],
    np.nan,
)

sum_distrib_df["cumul_last_price_net_dollar_flow_ended"] = np.where(
    sum_distrib_df["period"] == "Post-Program",
    sum_distrib_df["cumul_last_price_net_dollar_flow"],
    np.nan,
)
sum_distrib_df["net_flows_retention"] = np.where(
    sum_distrib_df["period"] == "Post-Program",
    sum_distrib_df["cumul_last_price_net_dollar_flow_ended"]
    / sum_distrib_df["cumul_last_price_net_dollar_flow_at_program_end_ended"],
    np.nan,
)
# If < 0 then make retention 0
sum_distrib_df["net_flows_retention"] = np.where(
    sum_distrib_df["cumul_last_price_net_dollar_flow"] < 0,
    0,
    sum_distrib_df["net_flows_retention"],
)

# Live Programs

sum_distrib_df["cumul_last_price_net_dollar_flow_at_program_end_live"] = np.where(
    sum_distrib_df["period"] == "During Program",
    sum_distrib_df["cumul_last_price_net_dollar_flow_at_program_end"],
    np.nan,
)

sum_distrib_df["cumul_last_price_net_dollar_flow_live"] = np.where(
    sum_distrib_df["period"] == "During Program",
    sum_distrib_df["cumul_last_price_net_dollar_flow"],
    np.nan,
)


# In[15]:


# Get App Name Mappings from Notion
name_mappings = program_df[["App Name", "App Name Map Override"]].drop_duplicates()
name_mappings = name_mappings[~name_mappings["App Name Map Override"].isna()]
name_mappings = name_mappings.rename(columns={"App Name": "agg_app_name"})
sum_distrib_df = sum_distrib_df.merge(name_mappings, on="agg_app_name", how="left")
sum_distrib_df["App Name Map"] = (
    sum_distrib_df["App Name Map Override"].combine_first(
        sum_distrib_df["agg_app_name"]
    )
).str.lower()

sum_distrib_df["last_updated"] = pd.to_datetime(datetime.datetime.now())

# Only export apps with a TVL
sum_distrib_df_export = sum_distrib_df[sum_distrib_df["top_level_name"] != 0]
sum_distrib_df_export.to_csv("csv_outputs/incentives_stats_summary.csv")
display(sum_distrib_df_export)
# display(sum_distrib_df[sum_distrib_df['agg_app_name'].str.contains('rrakis')])


# In[16]:


# sum_distrib_df_grouped = sum_distrib_df


# In[17]:


# assuming your dataframe is named `df`
program_df["App Name Map"] = (
    program_df["App Name Map Override"].combine_first(program_df["App Name"])
).str.lower()
program_df_grouped = (
    program_df.groupby("App Name Map")
    .agg({"# OP Allocated": "sum", "Source": lambda x: list(set(x))})
    .reset_index()
)

# reset_index() is used to convert the grouped result back to a dataframe
# the lambda function for 'Source' column aggregates text entries as a list/array
# display(program_df_grouped)

# Get Deployments Grouped
sum_distrib_df_grouped = sum_distrib_df.groupby("App Name Map").agg(sum).reset_index()
sum_distrib_df_grouped = sum_distrib_df_grouped.reset_index()
sum_distrib_df_grouped["net_flows_retention"] = np.where(
    sum_distrib_df_grouped["cumul_last_price_net_dollar_flow_ended"] < 0,
    0,
    sum_distrib_df_grouped["cumul_last_price_net_dollar_flow_ended"]
    / sum_distrib_df_grouped["cumul_last_price_net_dollar_flow_at_program_end_ended"],
)
sum_distrib_df_grouped.drop(
    [
        "num_op_override",
        "cumul_net_dollar_flow_at_program_end",
        "cumul_net_dollar_flow",
    ],
    axis=1,
    inplace=True,
)

# purpose is to make sure we don't double count tokens out
# From the app presepctive, we look at deployed
# From the overall / by season prespective, we look at net
sum_distrib_df_grouped["op_net_deployed"] = (
    sum_distrib_df_grouped["op_deployed"]
    - sum_distrib_df_grouped["op_from_other_projects"]
)

display(sum_distrib_df_grouped)
sum_distrib_df_grouped.to_csv("csv_outputs/incentives_summary_by_app.csv")
print(sum_distrib_df_grouped["op_net_deployed"].sum())

print(sum_distrib_df_grouped.columns)


# In[18]:


joined_df = sum_distrib_df_grouped.merge(
    program_df_grouped, on="App Name Map", how="outer"
)
display(joined_df)


# In[19]:


app_cols = [
    "app_name",
    "txs_per_day_prev",
    "num_addr_per_day_prev",
    "gas_fee_eth_per_day_prev",
    "txs_per_day",
    "num_addr_per_day",
    "gas_fee_eth_per_day",
    "txs_per_day_after",
    "num_addr_per_day_after",
    "gas_fee_eth_per_day_after",
]
app_df_sm = app_df[app_cols]
app_df_sm["app_name"] = app_df_sm["app_name"].str.lower()
app_df_sm = app_df_sm.rename(columns={"app_name": "App Name Map"})


# In[20]:


joined_df = joined_df.merge(app_df_sm, on="App Name Map", how="outer")

display(joined_df)

joined_df.to_csv("csv_outputs/total_stats_summary_by_app.csv")

