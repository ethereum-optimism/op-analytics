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


import math

def convert_to_int_or_keep_string(value):
    try:
        float_value = float(value)
        if math.isnan(float_value):  # Check if value is NaN
            return None
        if float_value.is_integer():
            val = str(int(float_value))  # Convert to int and then to string
        else:
            val = str(value)  # Convert to string
    except ValueError:
        val = str(value)  # Convert to string
    if val.endswith('.0'):
        val = val[:-2]  # Remove the last two characters
    return val


# In[ ]:


# Trim columns
df.columns = df.columns.str.replace(" ", "").str.strip()
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# Datetime
df['public_mainnet_launch_date'] = pd.to_datetime(df['public_mainnet_launch_date'], errors='coerce')
df['op_chain_start'] = pd.to_datetime(df['op_chain_start'], errors='coerce')
df['op_governed_start'] = pd.to_datetime(df['op_governed_start'], errors='coerce')
# ChainID
# Apply the function to the column
df['mainnet_chain_id'] = df['mainnet_chain_id'].apply(convert_to_int_or_keep_string)
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


#check chain id
df[df['display_name'] =='OP Mainnet']


# In[ ]:


# Post to Dune API
d.write_dune_api_from_pandas(df, table_name + '_info_tracking',\
                             'Basic Info & Metadata about OP Stack Chains, including forks')
#BQ Upload
bqu.write_df_to_bq_table(df, table_name)

#CH Upload
ch.write_df_to_clickhouse(df, table_name, if_exists='replace')

