#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import sys
sys.path.append("../../helper_functions")
import opstack_metadata_utils as ops
import duneapi_utils as d
import google_bq_utils as bqu
import clickhouse_utils as ch
sys.path.pop()

import dotenv
import os
dotenv.load_dotenv()


# In[ ]:


# Read the CSV file
df = pd.read_csv('chain_metadata_raw.csv')

table_name = 'op_stack_chain_metadata'


# In[ ]:


def convert_to_int_or_keep_string(value):
    try:
        # Try to convert to float first
        float_value = float(value)
        # Check if it's a whole number
        if float_value.is_integer():
            return int(float_value)  # Convert to int if it's a whole number
        else:
            return value  # Keep as original string if it has decimal places
    except ValueError:
        return value  # Keep as original string if it can't be converted to float


# In[ ]:


# Trim columns
df.columns = df.columns.str.replace(" ", "").str.strip()
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# Datetime
df['public_mainnet_launch_date'] = pd.to_datetime(df['public_mainnet_launch_date'], errors='coerce')
df['op_chain_start'] = pd.to_datetime(df['op_chain_start'], errors='coerce')
# ChainID
# Apply the function to the column
df['mainnet_chain_id'] = df['mainnet_chain_id'].apply(convert_to_int_or_keep_string)
df['mainnet_chain_id'] = df['mainnet_chain_id'].astype('string')
# df['mainnet_chain_id'] = int(df['mainnet_chain_id'])
#Generate Alignment Column
df = ops.generate_alignment_column(df)

# Replace NaN values in object type columns with an empty string
object_columns = df.select_dtypes(include=['object']).columns
df[object_columns] = df[object_columns].fillna('')

# Save the cleaned DataFrame to a new CSV file
df.to_csv('../outputs/chain_metadata.csv', index=False)


# In[ ]:


# df.dtypes


# In[ ]:


# df[df['mainnet_chain_id'] =='2702128']


# In[ ]:


# Post to Dune API
d.write_dune_api_from_pandas(df, table_name + '_info_tracking',\
                             'Basic Info & Metadata about OP Stack Chains, including forks')
#BQ Upload
bqu.write_df_to_bq_table(df, table_name)

#CH Upload
ch.write_df_to_clickhouse(df, table_name, if_exists='replace')

