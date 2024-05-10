#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import sys
sys.path.append("../../helper_functions")
import opstack_metadata_utils as ops
import duneapi_utils as d
sys.path.pop()

import dotenv
import os
dotenv.load_dotenv()


# In[ ]:


# Read the CSV file
df = pd.read_csv('chain_metadata_raw.csv')


# In[ ]:


# Trim columns
df.columns = df.columns.str.replace(" ", "").str.strip()
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# Datetime
df['public_mainnet_launch_date'] = pd.to_datetime(df['public_mainnet_launch_date'], errors='coerce')
#Generate Alignment Column
df = ops.generate_alignment_column(df)

# Replace NaN values in object type columns with an empty string
object_columns = df.select_dtypes(include=['object']).columns
df[object_columns] = df[object_columns].fillna('')

# Save the cleaned DataFrame to a new CSV file
df.to_csv('../outputs/chain_metadata.csv', index=False)


# In[ ]:


# df


# In[ ]:


# Post to Dune API
d.write_dune_api_from_pandas(df, 'op_stack_chain_metadata_info_tracking',\
                             'Basic Info & Metadata about OP Stack Chains, including forks')

