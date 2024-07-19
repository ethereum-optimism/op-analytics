#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import sys
sys.path.append("../helper_functions")
import clickhouse_utils as ch
import opstack_metadata_utils as ops
sys.path.pop()

import dotenv
import os
dotenv.load_dotenv()


# In[ ]:


# Get Chain List
chain_configs = ops.get_superchain_metadata_by_data_source('oplabs') # OPLabs db
# Should store this and add a check to see if the list changed before executing
# so that we're not rebuilding the view on every metadata update


# In[ ]:


# chain_configs


# In[ ]:


# Function to create ClickHouse view
def get_chain_names_from_df(df):
    return df['blockchain'].dropna().unique().tolist()

# Function to create ClickHouse view
def create_clickhouse_view(view_slug, dataset_type, chain_names, client = None):
    if client is None:
        client = ch.connect_to_clickhouse_db()

    query = f"CREATE OR REPLACE VIEW {view_slug}_{dataset_type} AS\n"
    union_queries = []
    
    for chain in chain_names:
        table_name = f"{chain}_{dataset_type}"
        if dataset_type == 'transactions':
            union_queries.append(f"""
                                SELECT 
                                id, hash, nonce, block_hash, block_number, transaction_index, from_address, to_address
                                , value, gas, gas_price, input, max_fee_per_gas, max_priority_fee_per_gas, transaction_type
                                , block_timestamp, receipt_cumulative_gas_used, receipt_gas_used, receipt_contract_address
                                , receipt_status, receipt_l1_fee, receipt_l1_gas_used, receipt_l1_gas_price, receipt_l1_fee_scalar
                                , receipt_l1_blob_base_fee, receipt_l1_blob_base_fee_scalar, blob_versioned_hashes, max_fee_per_blob_gas
                                , receipt_l1_block_number, receipt_l1_base_fee_scalar, chain, network, chain_id, insert_time
                                FROM {table_name} WHERE is_deleted = 0
                                """)
        else: 
            union_queries.append(f"""
                                SELECT 
                                *
                                FROM {table_name} WHERE is_deleted = 0
                                """)
    
    query += " UNION ALL\n".join(union_queries)

    # print(query)
    
    client.command(query)

    print(f"View '{view_slug}_{dataset_type}' created successfully.")


# In[ ]:


view_slug = 'superchain'
dataset_types = ['transactions', 'traces', 'blocks', 'logs']
chain_names = get_chain_names_from_df(chain_configs)
print(chain_names)
for dataset_type in dataset_types:
        create_clickhouse_view(view_slug, dataset_type, chain_names)

