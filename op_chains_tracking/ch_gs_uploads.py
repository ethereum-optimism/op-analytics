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
import opstack_metadata_utils as ops
sys.path.pop()

import time


# In[ ]:


client = ch.connect_to_clickhouse_db() #Default is OPLabs DB
# client.close()

table_name = 'daily_aggegate_l2_chain_usage_goldsky'


# In[ ]:


block_time_sec = 2

trailing_days = 180
max_execution_secs = 3000


# In[ ]:


# chain_mappings_list = [
#     # {'schema_name': 'zora', 'display_name': 'Zora', 'has_blob_fields': True},
#     # {'schema_name': 'pgn', 'display_name': 'Public Goods Network', 'has_blob_fields': True},
#     # {'schema_name': 'base', 'display_name': 'Base', 'has_blob_fields': False},
#     # {'schema_name': 'op', 'display_name': 'OP Mainnet', 'has_blob_fields': True},
#     {'schema_name': 'mode', 'display_name': 'Mode', 'has_blob_fields': True},
#     {'schema_name': 'metal', 'display_name': 'Metal', 'has_blob_fields': True},
#     {'schema_name': 'fraxtal', 'display_name': 'Fraxtal', 'has_blob_fields': True},
#     # {'schema_name': 'bob', 'display_name': 'BOB (Build on Bitcoin)', 'has_blob_fields': False},
#     {'schema_name': 'cyber', 'display_name': 'Cyber', 'has_blob_fields': True},
#     {'schema_name': 'mint', 'display_name': 'Mint', 'has_blob_fields': True},
#     # Add more mappings as needed
# ]
# chain_mappings_dict = {item['schema_name']: item['display_name'] for item in chain_mappings_list}


# In[ ]:


# Get Chain List
chain_configs = ops.get_op_stack_metadata_by_data_source('oplabs') # OPLabs db
#Filter to Superchain
chain_configs = chain_configs[chain_configs['alignment']=='OP Chain']

chain_configs


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
        for index, chain in chain_configs.iterrows():
                chain_schema = chain['blockchain']
                display_name = chain['display_name']
                # Read the SQL query from file
                with open(os.path.join(sql_directory, f"{qn}.sql"), "r") as file:
                        query = file.read()
                print(qn + ' - ' + chain_schema)

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
                result_df['chain'] = display_name
                unified_dfs.append(result_df)

        write_df = pd.concat(unified_dfs)
        write_df.to_csv('outputs/chain_data/' + qn + '.csv', index=False)
        
        # # # Print the results


# In[ ]:


# write_df.dtypes
write_df['chain_id'] = write_df['chain_id'].astype('int64')


# In[ ]:


#BQ Upload
time.sleep(1)
bqu.append_and_upsert_df_to_bq_table(write_df, table_name, unique_keys = ['dt','chain','network'])


# In[ ]:


# Copy to Dune
print('upload bq to dune')
sql = '''
SELECT *
FROM `api_table_uploads.daily_aggegate_l2_chain_usage_goldsky`
WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 366 DAY)
'''
bq_df = bqu.run_query_to_df(sql)

dune_table_name = 'ch_template_alltime_chain_activity'
d.write_dune_api_from_pandas(bq_df, dune_table_name,table_description = dune_table_name)

