#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os

from functools import reduce
from pandas.api.types import CategoricalDtype
from datetime import datetime, timedelta
from utils import format_number
from IPython.display import display  # So that display is recognized in .py files

from config import LAST_N_DAYS, COL_NAMES_TO_INCLUDE

import plotly.express as px
import plotly.graph_objects as go

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_columns", None)


# In[ ]:


pwd = os.getcwd()
# Verify that our path is right
if "op_rewards_tracking" in pwd:
    prepend = ""
else:
    prepend = "op_rewards_tracking/"


# In[ ]:


def extract_source(source_string):
    source_list = source_string.split("-")
    if len(source_list) > 1:
        return source_list[
            1
        ].strip()  # strip() is used to remove any leading/trailing spaces
    else:
        return source_string.strip()


def cleanup_string(source_string):
    return str(source_string).replace(" ", "").lower()


def remove_brackets(x):
    if isinstance(x, str):
        return x.replace("['", "").replace("']", "")
    else:
        return x


def merge_dfs(key="app_name_join", cols=COL_NAMES_TO_INCLUDE, **dfs):
    df_combined = reduce(
        lambda left, right: pd.merge(left, right, on=key, how="left"), dfs.values()
    )
    return df_combined[cols]


def calculate_metrics(df, op="op_deployed"):
    inc_cols = df.filter(like="incremental_").columns
    inc_cols = [col for col in inc_cols if "per_op" not in col]
    # df = df.assign(**{f'incremental_{col.split("_")[1]}_annualized_per_op': df[col] * 365 / df["net_op_deployed"] for col in inc_cols})
    df = df.assign(
        **{
            f'{col.replace("_per_day", "")}_annualized_per_op': df[col] * 365 / df[op]
            for col in inc_cols
        }
    )
    df["net_tvl_per_op"] = df["cumul_last_price_net_dollar_flow"] / df[op]
    df["net_tvl_per_op_during"] = (
        df["cumul_last_price_net_dollar_flow_at_program_end"] / df[op]
    )

    return df


# # Incentive Program Summary
# Status of programs live, completed and to be announced by season.

# In[ ]:


df_info = pd.read_csv("csv_outputs/" + "notion_automation_test" + ".csv")

# convert to datetime
df_info["start_date"] = pd.to_datetime(
    df_info["Announced On"].fillna(df_info["Start Date"])
)
df_info["end_date"] = pd.to_datetime(df_info["End Date"])

# convert program status into ordered categorical type
cat_size_order = CategoricalDtype(
    ["Live ‎🔥", "Coming soon ‎⏳", "Completed"], ordered=True
)
df_info["Status"] = df_info["Status"].astype(cat_size_order)

# create app_name_join, coalesce with app name override map, app name and remove any space
df_info["app_name_join"] = df_info["App Name Map Override"].fillna(df_info["App Name"])
df_info["app_name_join"] = df_info["app_name_join"].apply(cleanup_string)


# In[ ]:


for i in ["GovFund", "GovFund Growth Experiments", "All Programs"]:
    # Assign the filters
    if i == "GovFund":
        filter_name = " - GovFund Only"
        df_choice = df_info[df_info["Source"] != "['Partner Fund']"].copy()
    elif i == "GovFund Growth Experiments":
        filter_name = " - GovFund Growth Exp."
        df_choice = df_info[df_info["Source"] != "['Partner Fund']"].copy()
        df_choice = df_choice[
            df_choice["Incentive / Growth Program Included?"] == "Yes"
        ]
    else:
        filter_name = ""
        df_choice = df_info.copy()

    # clean up for columns needed
    df_choice = df_choice[
        [
            "Source",
            "Status",
            "# OP Allocated",
            "App Name",
            "start_date",
            "end_date",
            "app_name_join",
            "Incentive / Growth Program Included?",
        ]
    ]

    # remove square brackets and quotation marks from strings
    df_choice["Source"] = df_choice["Source"].str.replace(r"[\[\]']", "", regex=True)

    summary = pd.pivot_table(
        df_choice,
        values=["# OP Allocated", "App Name"],
        index=["Status", "Source"],
        aggfunc={"# OP Allocated": "sum", "App Name": "count"},
    )

    subtotal_name = "Subtotal" + filter_name
    # calculate subtotals on program status
    result = pd.concat(
        [
            summary,
            summary.groupby(level=0)
            .sum()
            .assign(item_name=subtotal_name)
            .set_index("item_name", append=True),
        ]
    ).sort_index(level=[0, 1])
    result = result.sort_index(level=[0, 1], ascending=[True, False])

    # add grand total to summary
    result.loc[("Grand Total"), "# OP Allocated"] = summary["# OP Allocated"].sum()
    result.loc[("Grand Total"), "App Name"] = summary["App Name"].sum()

    # cleanup display
    result["# Programs"] = result["App Name"].astype(int)
    result["# OP Allocated (M)"] = result["# OP Allocated"].apply(format_number)

    # calculate percentage of total
    result.loc[(slice(None), subtotal_name), "# OP Allocated"] / summary[
        "# OP Allocated"
    ].sum()
    result["% OP Allocated"] = (
        round(
            result.loc[(slice(None), subtotal_name), "# OP Allocated"]
            / summary["# OP Allocated"].sum()
            * 100
        )
        .astype(str)
        .replace("\.0", "", regex=True)
        + "%"
    )
    result["% OP Allocated"].fillna("-", inplace=True)

    result = result.replace((0, "0.0M", "0.0"), "-")
    print(i)
    display(result.drop(columns=["# OP Allocated", "App Name"]))
    print()


# In[ ]:


# display new programs in last 30 days
df_new_programs = df_choice[
    df_choice["start_date"] > pd.Timestamp("today") - timedelta(days=LAST_N_DAYS)
].sort_values(by="start_date", ascending=False)
if not df_new_programs.empty:
    df_new_programs["end_date"].fillna("-", inplace=True)
    display(df_new_programs.drop("app_name_join", axis=1))


# In[ ]:


# display completed programs in last 30 days
df_completed = df_choice[
    (df_choice["Status"] == "Completed")
    & (df_choice["end_date"] > pd.Timestamp("today") - timedelta(days=LAST_N_DAYS))
].sort_values(by="start_date", ascending=False)
if not df_completed.empty:
    display(df_completed.drop("app_name_join", axis=1))


# # Usage and TVL Attribution
# To combine all sources of data together

# In[ ]:


# read in input data
df_usage = pd.read_csv("csv_outputs/" + "dune_op_program_performance_summary" + ".csv")
# convert to datetime
df_usage["start_date"] = pd.to_datetime(df_usage["start_date"])
df_usage["end_date"] = pd.to_datetime(df_usage["end_date"])

df_usage["app_name_join"] = df_usage["app_name_a"].apply(cleanup_string)
df_usage["duration_days"] = (
    df_usage["end_date"].fillna(datetime.now()) - df_usage["start_date"]
).dt.days + 1  # if start and end date is the same, add 1 to include that day

# drop op_deployed from df_usage to avoid duplicates
df_usage = df_usage.drop(columns=["op_deployed"])

df_tvl = pd.read_csv("csv_outputs/op_summer_latest_stats.csv")
df_tvl = df_tvl[df_tvl["include_in_summary"] == 1]
df_tvl["app_name_join"] = df_tvl["parent_protocol"].apply(cleanup_string)

df_op_distribution = pd.read_csv("csv_outputs/dune_op_distribution_type.csv")
df_op_distribution["net_op_deployed"] = (
    df_op_distribution["op_deployed"] - df_op_distribution["op_from_other_projects"]
).astype(float)
df_op_distribution["app_name_join"] = df_op_distribution["project_name"].apply(
    cleanup_string
)

# filter to incentive / growth programs only
condition = (df_choice["Incentive / Growth Program Included?"] == "Yes") & (
    df_choice["start_date"].notnull()
)
df_choice = df_choice[condition]


# In[ ]:


df_to_summarize = {
    # df | groupby | column to summarize
    "df_choice": ("app_name_join", "# OP Allocated"),
    "df_tvl": (
        "app_name_join",
        [
            "cumul_last_price_net_dollar_flow",
            "cumul_last_price_net_dollar_flow_at_program_end",
        ],
    ),
    "df_op_distribution": ("app_name_join", ["op_deployed", "net_op_deployed"]),
}

summary_dfs = {}  # create an empty dictionary to store the resulting DataFrames

for df_name, (groupby_col, sum_cols) in df_to_summarize.items():
    df = globals()[df_name]  # assuming the dataframes are stored as global variables
    if isinstance(sum_cols, str):  # if only one column to sum is specified
        sum_cols = [sum_cols]
    # groupby and sum the specified columns
    grouped = df.groupby(groupby_col)[sum_cols].sum().reset_index()
    # create a new variable with the summary DataFrame
    first_word = groupby_col.split("_")[0]
    summary_df_name = f"{df_name}_summary_{first_word}"
    summary_dfs[summary_df_name] = grouped

# unpack summary_dfs into separate variables with the same names
locals().update(summary_dfs)

# access each summary DataFrame by its variable name
# df_choice_summary_app
# df_tvl_summary_app
# df_op_distribution_summary_app


# ### By App

# In[ ]:


# by app
df_combined_app = merge_dfs(
    df_usage = df_usage.loc[:, ~df_usage.columns.isin(['cumul_last_price_net_dollar_flow'])], # we get cumul_last_price_net_dollar_flow from TVl, since the Dune run has previous data
    df_tvl_summary_app=df_tvl_summary_app,
    df_choice_summary_app=df_choice_summary_app,
    df_op_distribution_summary_app=df_op_distribution_summary_app,
)

# # if op_deployed higher than op allocated, set to op allocated value
# mask = df_combined_app['op_deployed'] > df_combined_app['# OP Allocated']
# df_combined_app.loc[mask, 'op_deployed'] = df_combined_app.loc[mask, '# OP Allocated']

df_combined_app = df_combined_app.dropna(subset=["# OP Allocated"])

# calculate metrics
result_app = calculate_metrics(
    df_combined_app, op="op_deployed"
)  # by app use op_deployed
# display(result_app)


# In[ ]:


# sort by tvl
cols = [
    "app_name_a",
    "# OP Allocated",
    "op_deployed",
    "cumul_last_price_net_dollar_flow",
    "net_tvl_per_op",
]
display(
    result_app[cols]
    .sort_values("cumul_last_price_net_dollar_flow", ascending=False)
    .reset_index(drop=True)
    .head(10)
)


# In[ ]:


# sort by txs
txs_cols = [
    "app_name_a",
    "# OP Allocated",
    "op_deployed",
    "incremental_txs_per_day",
    "incremental_txs_annualized_per_op",
    "incremental_txs_per_day_after",
    "incremental_txs_after_annualized_per_op",
]

# # result_app[txs_cols].to_csv('csv_outputs/transaction_stats_by_app.csv')

display(
    result_app[txs_cols]
    .sort_values("incremental_txs_per_day", ascending=False)
    .reset_index(drop=True)
    .head(10)
)

display(
    result_app[txs_cols]
    .sort_values("incremental_txs_after_annualized_per_op", ascending=False)
    .dropna()
    .reset_index(drop=True)
    .head(10)
)


# In[ ]:


# sort by gas
gas_cols = [
    "app_name_a",
    "# OP Allocated",
    "op_deployed",
    "incremental_gas_fee_eth_per_day",
    "incremental_gas_fee_eth_annualized_per_op",
    "incremental_gas_fee_eth_per_day_after",
    "incremental_gas_fee_eth_after_annualized_per_op",
]

result_app.loc[
    :, result_app.columns.str.contains("annualized_per_op")
] = result_app.loc[:, result_app.columns.str.contains("annualized_per_op")].applymap(
    "{:.4f}".format
)

display(
    result_app[gas_cols]
    .sort_values("incremental_gas_fee_eth_per_day", ascending=False)
    .reset_index(drop=True)
    .head(10)
)

display(
    result_app[gas_cols]
    .sort_values("incremental_gas_fee_eth_after_annualized_per_op", ascending=False)
    .dropna()
    .reset_index(drop=True)
    .head(10)
)


# ### By Fund Source

# In[ ]:


agg_dict = {
    "# OP Allocated": "sum",
    "net_op_deployed": "sum",
    # "incremental_addr_per_day": "sum",
    "incremental_txs_per_day": "sum",
    "incremental_gas_fee_eth_per_day": "sum",
    "incremental_txs_per_day_after": "sum",
    # "incremental_addr_per_day_after": "sum",
    "incremental_gas_fee_eth_per_day_after": "sum",
    "cumul_last_price_net_dollar_flow": "sum",
    "cumul_last_price_net_dollar_flow_at_program_end": "sum",
}


# In[ ]:


result_app["op_source_length"] = result_app["op_source"].str.split(",").apply(len)
result_app["op_source_map"] = np.where(
    result_app["op_source_length"] > 1, ["Multiple"], result_app["op_source"]
)

result_source = result_app.groupby("op_source_map").agg(agg_dict)

# calculate metrics
result_source = calculate_metrics(
    result_source, op="net_op_deployed"
)  # use net to avoid double counting
result_source = result_source.reset_index()
result_source["op_source_map"] = result_source["op_source_map"].apply(
    lambda x: remove_brackets(x)
)
result_source.sort_values("op_source_map").reset_index()

display(result_source)


# 

# In[ ]:


# convert results to csv
result_app.to_csv("csv_outputs/final_incentive_program_summary_by_app.csv")


# ### Benchmark

# In[ ]:


def plot_benchmark(
    df,
    layout_settings,
    x="incremental_txs_per_day",
    y="incremental_txs_annualized_per_op",
    size="op_deployed",
):
    fig = px.scatter(
        df,
        x=x,
        y=y,
        size=size,
        hover_name="app_name_a",
        color="op_source_map",
    )

    # calculate percentiles for incremental_txs_annualized_per_op
    p25 = df[y].quantile(0.25)
    p50 = df[y].quantile(0.50)
    p75 = df[y].quantile(0.75)

    x_range = [df[x].min(), df[x].max()]

    # add vertical lines for percentiles
    fig.add_trace(
        go.Scatter(x=x_range, y=[p25, p25], mode="lines", name="25th percentile")
    )
    fig.add_trace(
        go.Scatter(x=x_range, y=[p50, p50], mode="lines", name="50th percentile")
    )
    fig.add_trace(
        go.Scatter(x=x_range, y=[p75, p75], mode="lines", name="75th percentile")
    )

    fig.update_layout(layout_settings)

    fig.write_image(prepend + f"img_outputs/benchmark/svg/{y}.svg")
    fig.write_image(prepend + f"img_outputs/benchmark/png/{y}.png")
    fig.write_html(
        prepend + f"img_outputs/benchmark/html/{y}.html", include_plotlyjs="cdn"
    )

    fig.show()


def cleanup_data(
    df=result_app,
    subset=[
        "op_deployed",
        "incremental_txs_annualized_per_op",
        "incremental_txs_per_day",
    ],
    excl_partnerfund=False,
):
    df = result_app.dropna(subset=subset)
    df = df.replace([np.inf, -np.inf], np.nan).dropna(
        subset=subset
    )  # remove rows with infinity values

    if excl_partnerfund:
        # drop anything with Partner Fund from df
        df = df[~df["op_source"].str.contains("Partner Fund")]

    df[subset] = df[subset].apply(pd.to_numeric, errors="coerce")

    return df


# ### Transactions Benchmark

# In[ ]:


layout_settings = {
    "title": "Incremental Txs Performance Benchmark (All Programs)<br><sup>Cutoff at Program End Date (Latest Date if still Live).</sup>",
    "xaxis_title": "Incremental Transactions per Day",
    "yaxis_title": "Annualized Incremental Transactions per OP",
    "legend_title": "Op Source",
}

df = cleanup_data()

plot_benchmark(
    df,
    x="incremental_txs_per_day",
    y="incremental_txs_annualized_per_op",
    size="op_deployed",
    layout_settings=layout_settings,
)


# In[ ]:


layout_settings = {
    "title": "Incremental Txs Performance Benchmark (Completed Programs)<br><sup>Cutoff at Program End Date + 30 days (Latest Date if not yet reached 30 days).</sup>",
    "xaxis_title": "Incremental Transactions per Day",
    "yaxis_title": "Annualized Incremental Transactions per OP",
    "legend_title": "Op Source",
}

df = cleanup_data(
    subset=[
        "op_deployed",
        "incremental_txs_per_day_after",
        "incremental_txs_after_annualized_per_op",
    ],
)

plot_benchmark(
    df,
    x="incremental_txs_per_day_after",
    y="incremental_txs_after_annualized_per_op",
    size="op_deployed",
    layout_settings=layout_settings,
)


# ### TVL Benchmark

# In[ ]:


layout_settings = {
    "title": "Incremental TVL Performance Benchmark (All Programs)<br><sup>Cutoff at Program End Date (Latest Date if still Live).</sup>",
    "xaxis_title": "Incremental TVL",
    "yaxis_title": "Incremental TVL per OP",
    "legend_title": "Op Source",
}

df = cleanup_data(
    subset=[
        "op_deployed",
        "net_tvl_per_op_during",
        "cumul_last_price_net_dollar_flow_at_program_end",
    ]
)

plot_benchmark(
    df,
    x="cumul_last_price_net_dollar_flow_at_program_end",
    y="net_tvl_per_op_during",
    size="op_deployed",
    layout_settings=layout_settings,
)


# In[ ]:


layout_settings = {
    "title": "Incremental TVL Performance Benchmark (Completed Programs) <br><sup>Cutoff at Program End Date + 30 days (Latest Date if not yet reached 30 days).</sup>",
    "xaxis_title": "Incremental TVL",
    "yaxis_title": "Incremental TVL per OP",
    "legend_title": "Op Source",
}

df = cleanup_data(
    subset=[
        "op_deployed",
        "net_tvl_per_op",
        "cumul_last_price_net_dollar_flow",
        "incremental_txs_per_day_after",  # used for filtering completed programs only
    ]
)

plot_benchmark(
    df,
    x="cumul_last_price_net_dollar_flow",
    y="net_tvl_per_op",
    size="op_deployed",
    layout_settings=layout_settings,
)


# ### Fee Benchmark

# In[ ]:


layout_settings = {
    "title": "Incremental ETH Fee Performance Benchmark (All Programs) <br><sup>Cutoff at Program End Date (Latest Date if still Live).</sup>",
    "xaxis_title": "Incremental ETH Fee per Day",
    "yaxis_title": "Annualized Incremental Fee per OP",
    "legend_title": "Op Source",
}

df = cleanup_data(
    subset=[
        "op_deployed",
        "incremental_gas_fee_eth_per_day",
        "incremental_gas_fee_eth_annualized_per_op",
    ]
)

plot_benchmark(
    df,
    x="incremental_gas_fee_eth_per_day",
    y="incremental_gas_fee_eth_annualized_per_op",
    size="op_deployed",
    layout_settings=layout_settings,
)


# In[ ]:


layout_settings = {
    "title": "Incremental ETH Fee Performance Benchmark (Completed Programs) <br><sup>Cutoff at Program End Date + 30 days (Latest Date if not yet reached 30 days).</sup>",
    "xaxis_title": "Incremental ETH Fee per Day",
    "yaxis_title": "Annualized Incremental Fee per OP",
    "legend_title": "Op Source",
}

df = cleanup_data(
    subset=[
        "op_deployed",
        "incremental_gas_fee_eth_per_day_after",
        "incremental_gas_fee_eth_after_annualized_per_op",
    ]
)

plot_benchmark(
    df,
    x="incremental_gas_fee_eth_per_day_after",
    y="incremental_gas_fee_eth_after_annualized_per_op",
    size="op_deployed",
    layout_settings=layout_settings,
)

