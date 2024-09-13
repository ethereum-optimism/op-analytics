#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import sys
import datetime
import time
sys.path.append("../../helper_functions")
import clickhouse_utils as ch
import opstack_metadata_utils as ops
import goldsky_db_utils as gsb
sys.path.pop()
client = ch.connect_to_clickhouse_db()

import dotenv
import os
dotenv.load_dotenv()


# In[ ]:


# Get Chain List
chain_configs = ops.get_superchain_metadata_by_data_source('oplabs') # OPLabs db

# chain_configs = chain_configs[chain_configs['chain_name'] == 'kroma']

chains = chain_configs['blockchain'].dropna().unique().tolist()

if client is None:
        client = ch.connect_to_clickhouse_db()

# chain_configs


# In[ ]:


table_projections = {
    'transactions': [
        ('proj_block_timestamp', ['block_timestamp', 'id']),
        ('proj_block_number', ['block_number', 'id']),
        ('proj_block_timestamp_number', ['block_timestamp', 'block_number', 'id']),
    ],
    'blocks': [
        ('proj_timestamp', ['timestamp', 'id']),
        ('proj_number', ['number', 'id']),
        ('proj_timestamp_number', ['timestamp', 'number', 'id']),
    ],
    'logs': [
        ('proj_block_timestamp', ['block_timestamp', 'id']),
        ('proj_block_number', ['block_number', 'id']),
        ('proj_block_timestamp_number', ['block_timestamp', 'block_number', 'id']),
    ],
    'traces': [
        ('proj_block_timestamp', ['block_timestamp', 'id']),
        ('proj_block_number', ['block_number', 'id']),
        ('proj_block_timestamp_number', ['block_timestamp', 'block_number', 'id']),
    ],
}


# In[ ]:


def create_projection_if_not_exists(client, table_name, projection_name, projection_fields):
    # Check if projection exists
    check_query = f"SHOW CREATE TABLE {table_name}"
    result = client.query(check_query)
    create_table_script = result.result_rows[0][0]

    if f"PROJECTION {projection_name}" not in create_table_script:
        print(f"Projection {projection_name} does not exist for table {table_name}. Creating...")
        
        # Create projection
        projection_fields_str = ", ".join(projection_fields)
        create_query = f"""
        ALTER TABLE {table_name}
        ADD PROJECTION {projection_name}
        (
            SELECT *
            ORDER BY ({projection_fields_str})
        )
        """
        
        client.command(create_query)
        
        # Materialize projection
        materialize_query = f"""
        ALTER TABLE {table_name}
        MATERIALIZE PROJECTION {projection_name}
        """
        
        client.command(materialize_query)
        
        print(f"Projection {projection_name} created and materialized successfully for table {table_name}.")
    else:
        print(f"Projection {projection_name} already exists for table {table_name}. Skipping creation.")


# In[ ]:


# Iterate over chains and tables
for chain in chains:
    for table, projections in table_projections.items():
        table_name = f"{chain}_{table}"
        for proj_name, proj_fields in projections:
            create_projection_if_not_exists(client, table_name, proj_name, proj_fields)


# In[ ]:


client.close()

