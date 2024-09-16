#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import sys
sys.path.append("../../helper_functions")
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
def create_clickhouse_view(view_slug, dataset_type, chain_names, client = None, do_is_deleted_line = True):
    if client is None:
        client = ch.connect_to_clickhouse_db()

    query = f"CREATE OR REPLACE VIEW {view_slug}_{dataset_type} AS\n"
    union_queries = []
    
    for chain in chain_names:
        table_name = f"{chain}_{dataset_type}"
        if dataset_type == 'transactions':
            sql_query = f"""
                                SELECT 
                                id, hash, nonce, block_hash, block_number, transaction_index, from_address, to_address
                                , value, gas, gas_price, input, max_fee_per_gas, max_priority_fee_per_gas, transaction_type
                                , block_timestamp, receipt_cumulative_gas_used, receipt_gas_used, receipt_contract_address
                                , receipt_status, receipt_l1_fee, receipt_l1_gas_used, receipt_l1_gas_price, receipt_l1_fee_scalar
                                , receipt_l1_blob_base_fee, receipt_l1_blob_base_fee_scalar, blob_versioned_hashes, max_fee_per_blob_gas
                                , receipt_l1_block_number, receipt_l1_base_fee_scalar, chain, network, chain_id, insert_time
                                FROM {table_name}
                                WHERE 1=1
                                """
        else: 
            sql_query = f"""
                                SELECT 
                                *
                                FROM {table_name}
                                WHERE 1=1
                                """
            
        if do_is_deleted_line:
                sql_query += ' AND is_deleted = 0'
        # finalize query
        union_queries.append(sql_query)
    
    query += " UNION ALL\n".join(union_queries)

    # print(query)
    
    client.command(query)

    print(f"View '{view_slug}_{dataset_type}' created successfully.")


# In[ ]:


view_slug = 'superchain'
native_dataset_types = [
                # native
                'transactions', 'traces', 'blocks', 'logs',
]

mv_dataset_types = [
                # mvs
                #  'erc20_transfers_mv','native_eth_transfers_mv',
                #  ,'transactions_unique'
                 'daily_aggregate_transactions_to_mv',
                 'across_bridging_txs_v3_mv',
                 'across_bridging_txs_v3_logs_only_mv',
                 ]
dataset_types = native_dataset_types + mv_dataset_types
print(dataset_types)
chain_names = get_chain_names_from_df(chain_configs)
print(chain_names)
for dataset_type in dataset_types:
        is_deleted_line = True
        try:
                if dataset_type in mv_dataset_types:
                        is_deleted_line = False
                create_clickhouse_view(view_slug, dataset_type, chain_names, do_is_deleted_line = is_deleted_line)
        except Exception as e:
                print(f'Can not create view for {dataset_type}')
                print(f'Error: {str(e)}')

