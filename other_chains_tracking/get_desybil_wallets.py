#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('get qualified txs')
import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
# import flipside_utils as f
# import clickhouse_utils as ch
import csv_utils as cu
import google_bq_utils as bqu
sys.path.pop()

import numpy as np
import pandas as pd
import os
# import clickhouse_connect as cc


# In[ ]:


# ch_client = ch.connect_to_clickhouse_db() #Default is OPLabs DB

query_name = 'daily_evms_desybilled_wallet_counts'

trailing_pds = 180


# In[ ]:


# flipside_configs = [
#         {'blockchain': 'blast', 'name': 'Blast', 'layer': 'L2', 'trailing_days': 365}
# ]
# clickhouse_configs = [
#         {'blockchain': 'metal', 'name': 'Metal', 'layer': 'L2', 'trailing_days': 365},
#         {'blockchain': 'mode', 'name': 'Mode', 'layer': 'L2', 'trailing_days': 365},
#         {'blockchain': 'bob', 'name': 'BOB (Build on Bitcoin)', 'layer': 'L2', 'trailing_days': 365},
#         {'blockchain': 'fraxtal', 'name': 'Fraxtal', 'layer': 'L2', 'trailing_days': 365},
# ]


# In[ ]:


# # Run Flipside
# print('     flipside runs')
# flip_dfs = []
# with open(os.path.join("inputs/sql/flipside_bychain.sql"), "r") as file:
#                         og_query = file.read()

# for chain in flipside_configs:
#         print(     'flipside: ' + chain['blockchain'])
#         query = og_query
#         #Pass in Params to the query
#         query = query.replace("@blockchain@", chain['blockchain'])
#         query = query.replace("@name@", chain['name'])
#         query = query.replace("@layer@", chain['layer'])
#         query = query.replace("@trailing_days@", str(chain['trailing_days']))
        
#         df = f.query_to_df(query)

#         flip_dfs.append(df)

# flip = pd.concat(flip_dfs)
# flip['source'] = 'flipside'
# flip['dt'] = pd.to_datetime(flip['dt']).dt.tz_localize(None)
# flip = flip[['dt','blockchain','name','layer','num_qualified_txs','source']]


# In[ ]:


# Run Dune
print('     dune runs')
days_param = d.generate_query_parameter(input=trailing_pds,field_name='trailing_num_periods',dtype='number')
dune_df = d.get_dune_data(query_id = 3784159, #https://dune.com/queries/3784159
    name = "dune_" + query_name,
    path = "outputs",
    performance="large",
    params = [days_param]
)
dune_df['source'] = 'dune'
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.tz_localize(None)


# In[ ]:


# # Run Clickhouse
# print('     clickhouse runs')
# ch_dfs = []
# with open(os.path.join("inputs/sql/goldsky_bychain.sql"), "r") as file:
#                         og_query = file.read()

# for chain in clickhouse_configs:
#         print(     'clickhouse: ' + chain['blockchain'])
#         query = og_query
#         #Pass in Params to the query
#         query = query.replace("@blockchain@", chain['blockchain'])
#         query = query.replace("@name@", chain['name'])
#         query = query.replace("@layer@", chain['layer'])
#         query = query.replace("@trailing_days@", str(chain['trailing_days']))
        
#         df = ch_client.query_df(query)

#         ch_dfs.append(df)

# ch = pd.concat(ch_dfs)
# ch['source'] = 'goldsky'
# ch['dt'] = pd.to_datetime(ch['dt']).dt.tz_localize(None)
# ch = ch[['dt','blockchain','name','layer','num_qualified_txs','source']]


# In[ ]:


# # Step 1: Filter dune_df for chains not in flip
# filtered_dune_df = dune_df[~dune_df['blockchain'].isin(flip['blockchain'])]
# # Step 2: Union flip and filtered_dune_df
# combined_flip_dune = pd.concat([flip, filtered_dune_df])
# # Step 3: Filter ch for chains not in combined_flip_dune
# filtered_ch = ch[~ch['blockchain'].isin(combined_flip_dune['blockchain'])]
# # Step 4: Union the result with filtered_ch
# final_df = pd.concat([combined_flip_dune, filtered_ch])
# # final_df
# # Temp until we pull in outher sources
final_df = dune_df.copy()


# In[ ]:


opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')

opstack_metadata['display_name_lower'] = opstack_metadata['display_name'].str.lower()
final_df['display_name_lower'] = final_df['name'].str.lower()

meta_cols = ['is_op_chain','mainnet_chain_id','op_based_version', 'alignment','chain_name', 'display_name','display_name_lower']

final_enriched_df = final_df.merge(opstack_metadata[meta_cols], on='display_name_lower', how = 'left')
final_enriched_df['alignment'] = final_enriched_df['alignment'].fillna('Other EVMs')
final_enriched_df['is_op_chain'] = final_enriched_df['is_op_chain'].fillna(False)
final_enriched_df['display_name'] = final_enriched_df['display_name'].fillna(final_enriched_df['name'])

final_enriched_df = final_enriched_df.drop(columns=['name'])


# In[ ]:


final_enriched_df.sort_values(by=['dt','blockchain'], ascending =[False, False], inplace = True)

final_enriched_df.to_csv('outputs/'+query_name+'.csv', index=False)


# In[ ]:


#BQ Upload
bqu.write_df_to_bq_table(final_enriched_df, query_name)

