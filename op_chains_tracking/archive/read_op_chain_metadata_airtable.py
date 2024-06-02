#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import airtable_utils as a
import pandas_utils as p
sys.path.pop()

import dotenv
import os
dotenv.load_dotenv()

at_base_id = os.environ["AIRTABLE_DEVREL_BASE_ID"]

columns = ['Name','Status','Alignment','Dune Database Name','Initial Chain Deployer','Block Explorer','Announcement Date', 'Mainnet Launch Date','Shutdown Date'
           ,'L1 Standard Bridge Contract', 'L1 NFT Bridge Contract', 'BatchInbox "to address"', 'BatchInbox "from address"'
           ,'L2 Output Oracle Proxy','L2 Output Oracle Underlying Contract','L2 Output Oracle "from address"'
           ,'Data Availability Layer']


# In[ ]:


# at = airtable.Airtable(at_base, at_api)
# data = at.get('OP Products')
# df = pd.json_normalize(data, record_path='records')
# # Rename all columns that start with 'fields.'
# df.rename(columns=lambda x: x.replace('fields.', ''), inplace=True)
df = a.get_dataframe_from_airtable_database(at_base_id,'OP Products')

# display(df[columns])
# Filter the DataFrame
filtered_df = df[
        (df['BatchInbox "to address"'].notnull()) & (df['BatchInbox "to address"'].str.startswith('0x'))
        & (df['Status'].notnull()) 
        & ( (df['Status'].astype(str).str.contains('Launched')) | (df['Status'].astype(str).str.contains('Shut Down')) )
        ]
# filtered_df.reset_index(inplace=True, drop=True)
filtered_df = filtered_df[columns]


# In[ ]:


# Format timeseries columns
# filtered_df = p.format_datetime_columns(filtered_df, format='%Y-%m-%dT%H:%M:%S.%fZ')
#Format bracket cols
# bracket_cols = ['Status','Alignment','Data Availability Layer']
# for b in bracket_cols:
#         df[b] = df[b].str.replace('[', '').str.replace(']', '')

# display(filtered_df)

# print(type(filtered_df['Status']))


# In[ ]:


filtered_df.to_csv('outputs/op_stack_chain_metadata.csv', index = False)

# Post to Dune API
d.write_dune_api_from_pandas(filtered_df, 'op_stack_chain_metadata',\
                             'Metadata about OP Stack Chains (i.e. bridge contracts, submitter contracts, name)')

