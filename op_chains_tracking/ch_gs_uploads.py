#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Clickhouse db w/ Goldsky
# https://clickhouse.com/docs/en/integrations/python

import requests as r
import pandas as pd
import clickhouse_connect as cc
import os

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import pandas_utils as p
import clickhouse_utils as ch
sys.path.pop()

import time

client = ch.connect_to_clickhouse_db() #Default is OPLabs DB
# client.close()


# In[2]:


chain_mappings = {
        # CH Schema Name, Display Name
        # 'zora': 'Zora',
        # 'pgn': 'Public Goods Network',
        # 'base': 'Base',
        # 'mode': 'Mode',
        # 'metal': 'Metal',
        'fraxtal': 'Fraxtal',
        # 'bob':'BOB',
        # Add more mappings as needed
    }
block_time_sec = 2

trailing_days = 9999
max_execution_secs = 3000


# In[3]:


sql_directory = "inputs/sql/"

query_names = [
        # Must match the file name in inputs/sql
        "ch_template_alltime_chain_activity"
]


# In[4]:


unified_dfs = []
table_name = 'op_ch_allltime_chain_activity'


# In[5]:


for qn in query_names:
        for key, value in chain_mappings.items():
                chain_schema = key
                display_name = value
                # If we can do it programmatically from UI saved queries
                # query = client.get_job(query_name)
                # Read the SQL query from file
                with open(os.path.join(sql_directory, f"{qn}.sql"), "r") as file:
                        query = file.read()
                print(qn + ' - ' + chain_schema)
                table_name = qn

                #Pass in Params to the query
                query = query.replace("@chain_db_name@", chain_schema)
                query = query.replace("@trailing_days@", str(trailing_days))
                query = query.replace("@block_time_sec@", str(block_time_sec))
                query = query.replace("@max_execution_secs@", str(max_execution_secs))

                # Execute the query
                result_df = client.query_df(query)
        #         # Write to csv
        #         df.to_csv('outputs/chain_data/' + qn + '.csv', index=False)
        #         # print(df.sample(5))
        #         time.sleep(1)
                
                result_df['chain_raw'] = result_df['chain']
                result_df['chain'] = result_df['chain'].replace(chain_mappings)
                unified_dfs.append(result_df)

        write_df = pd.concat(unified_dfs)
        write_df.to_csv('outputs/chain_data/' + table_name + '.csv', index=False)
        d.write_dune_api_from_pandas(write_df, table_name,table_description = table_name)
        
        # # # Print the results


# In[6]:


write_df.sample(5)

