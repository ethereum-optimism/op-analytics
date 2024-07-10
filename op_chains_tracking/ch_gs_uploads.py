#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start ch uploads')
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
import csv_utils as cu
import google_bq_utils as bqu
sys.path.pop()

import time


# In[ ]:


client = ch.connect_to_clickhouse_db() #Default is OPLabs DB
# client.close()

table_name = 'daily_aggegate_l2_chain_usage_goldsky'


# In[ ]:


chain_mappings_list = [
    # {'schema_name': 'zora', 'display_name': 'Zora', 'has_blob_fields': True},
    # {'schema_name': 'pgn', 'display_name': 'Public Goods Network', 'has_blob_fields': True},
    # {'schema_name': 'base', 'display_name': 'Base', 'has_blob_fields': False},
    # {'schema_name': 'op', 'display_name': 'OP Mainnet', 'has_blob_fields': True},
    {'schema_name': 'mode', 'display_name': 'Mode', 'has_blob_fields': True},
    {'schema_name': 'metal', 'display_name': 'Metal', 'has_blob_fields': True},
    {'schema_name': 'fraxtal', 'display_name': 'Fraxtal', 'has_blob_fields': True},
    {'schema_name': 'bob', 'display_name': 'BOB (Build on Bitcoin)', 'has_blob_fields': False},
    {'schema_name': 'cyber', 'display_name': 'Cyber', 'has_blob_fields': True},
    {'schema_name': 'mint', 'display_name': 'Mint', 'has_blob_fields': True},
    # Add more mappings as needed
]
chain_mappings_dict = {item['schema_name']: item['display_name'] for item in chain_mappings_list}

block_time_sec = 2

trailing_days = 180
max_execution_secs = 3000


# In[ ]:


sql_directory = "inputs/sql/"

query_names = [
        # Must match the file name in inputs/sql
        "ch_template_alltime_chain_activity"
]


# In[ ]:


unified_dfs = []


# In[ ]:


for qn in query_names:
        for mapping in chain_mappings_list:
                chain_schema = mapping['schema_name']
                display_name = mapping['display_name']
                has_blob_fields = mapping['has_blob_fields']
                # If we can do it programmatically from UI saved queries
                # query = client.get_job(query_name)
                # Read the SQL query from file
                if has_blob_fields:
                        qn_map=qn+ '_updated'
                else:
                        qn_map =qn
                with open(os.path.join(sql_directory, f"{qn_map}.sql"), "r") as file:
                        query = file.read()
                print(qn_map + ' - ' + chain_schema)
                dune_table_name = qn

                #Pass in Params to the query
                query = query.replace("@chain_db_name@", chain_schema)
                query = query.replace("@trailing_days@", str(trailing_days))
                query = query.replace("@block_time_sec@", str(block_time_sec))
                query = query.replace("@max_execution_secs@", str(max_execution_secs))

                if not has_blob_fields:
                        query = query.replace("receipt_l1_blob_base_fee_scalar", 'cast(NULL as Nullable(Float64))')
                        query = query.replace("receipt_l1_blob_base_fee", 'cast(NULL as Nullable(Float64))')
                        query = query.replace("receipt_l1_base_fee_scalar", 'toInt64(NULL)')
                # Execute the query
                result_df = client.query_df(query)
        #         # Write to csv
        #         df.to_csv('outputs/chain_data/' + qn + '.csv', index=False)
        #         # print(df.sample(5))
        #         time.sleep(1)
                
                result_df['chain_raw'] = result_df['chain']
                result_df['chain'] = result_df['chain'].replace(chain_mappings_dict)
                unified_dfs.append(result_df)

        write_df = pd.concat(unified_dfs)
        write_df.to_csv('outputs/chain_data/' + dune_table_name + '.csv', index=False)
        d.write_dune_api_from_pandas(write_df, dune_table_name,table_description = dune_table_name)
        
        # # # Print the results


# In[ ]:


write_df.dtypes


# In[ ]:


#BQ Upload
time.sleep(1)
bqu.append_and_upsert_df_to_bq_table(write_df, table_name, unique_keys = ['dt','chain','network'])

