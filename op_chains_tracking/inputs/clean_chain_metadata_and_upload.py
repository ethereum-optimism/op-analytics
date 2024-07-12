#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sys
sys.path.append("../../helper_functions")
import opstack_metadata_utils as ops
import duneapi_utils as d
import google_bq_utils as bqu
sys.path.pop()

import dotenv
import os
dotenv.load_dotenv()


# In[2]:


# Read the CSV file
df = pd.read_csv('chain_metadata_raw.csv')

table_name = 'op_stack_chain_metadata'


# In[3]:


# Trim columns
df.columns = df.columns.str.replace(" ", "").str.strip()
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# Datetime
df['public_mainnet_launch_date'] = pd.to_datetime(df['public_mainnet_launch_date'], errors='coerce')
# ChainID
df['mainnet_chain_id'] = pd.to_numeric(df['mainnet_chain_id'], errors='coerce')#.astype(int)
# df['mainnet_chain_id'] = int(df['mainnet_chain_id'])
#Generate Alignment Column
df = ops.generate_alignment_column(df)

# Replace NaN values in object type columns with an empty string
object_columns = df.select_dtypes(include=['object']).columns
df[object_columns] = df[object_columns].fillna('')

# Save the cleaned DataFrame to a new CSV file
df.to_csv('../outputs/chain_metadata.csv', index=False)


# In[4]:


# df


# In[5]:


# Post to Dune API
d.write_dune_api_from_pandas(df, table_name + '_info_tracking',\
                             'Basic Info & Metadata about OP Stack Chains, including forks')
#BQ Upload
bqu.write_df_to_bq_table(df, table_name)

