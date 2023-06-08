#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import notion_client
import sys

sys.path.append("../helper_functions")
import notionapi_utils as n
import duneapi_utils as d


# In[ ]:


op_summer_page = "https://www.notion.so/oplabs/26d856d5ad7c4fda919c62e839cf6051?v=96c651b94d564c37b96c7eca2bd91157&pvs=4"

# df = n.read_notion_database(op_summerdb_id)
df = n.read_notion_database_to_pandas_df(op_summer_page)
df.to_csv("csv_outputs/notion_automation_test.csv")
# display(df)


# In[ ]:


columns = [
    "Source",
    "Status",
    "App Name",
    "App Name Map Override",
    "App Type",
    "Announced On",
    "Start Date",
    "End Date",
    "# OP Allocated",
    "Actions Rewarded",
]

# filter for live or completed programs only
df = df[df["Status"] != "Coming soon ‎⏳"][columns]
df.reset_index(drop=True, inplace=True)


# In[ ]:


# remove square brackets and quotation marks from strings
for col in ["Source", "App Type", "Actions Rewarded"]:
    df[col] = df[col].astype(str).str.replace(r"[\[\]']", "", regex=True)

# create fund_type and op_source
df[["fund_type", "op_source"]] = df["Source"].astype(str).str.extract(r"^(.*?)(?:\s*-\s*(.*))?$")
df["op_source"].fillna(df["fund_type"], inplace=True)
df.drop(columns=["Source"], inplace=True)


# In[ ]:


# define the new column names
new_columns = {
    "Status": "status",
    "App Name": "app_name",
    "App Name Map Override": "app_name_override",
    "App Type": "category",
    "Announced On": "announce_date",
    "Start Date": "start_date",
    "End Date": "end_date",
    "# OP Allocated": "num_op_allocated",
    "Actions Rewarded": "incentive_type",
}
df = df.rename(columns=new_columns)
df["app_name_override"].fillna(df["app_name"], inplace=True)


# In[ ]:


# Post to Dune API
d.write_dune_api_from_pandas(
    df, "op_incentive_program_info", "Metadata about OP Incentive Programs"
)

