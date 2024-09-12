#!/usr/bin/env python
# coding: utf-8

# In[1]:


# List of materialized view names
mv_names = [
        # in order of build
        'erc20_transfers',
        'native_eth_transfers',
        # 'transactions_unique',

        'daily_aggregate_transactions_to',
        'across_bridging_txs_v3'
        ]
set_days_batch_size = 30

optimize_all = True


# In[2]:


import pandas as pd
import sys
import datetime
import time
import traceback
sys.path.append("../../helper_functions")
import clickhouse_utils as ch
import opstack_metadata_utils as ops
import goldsky_db_utils as gsb
sys.path.pop()
client = ch.connect_to_clickhouse_db()

import dotenv
import os
dotenv.load_dotenv()


# In[3]:


# Get Chain List
chain_configs = ops.get_superchain_metadata_by_data_source('oplabs') # OPLabs db

if client is None:
        client = ch.connect_to_clickhouse_db()

# Function to create ClickHouse view
def get_chain_names_from_df(df):
    return df['blockchain'].dropna().unique().tolist()

# chain_configs = chain_configs[chain_configs['chain_name'] == 'bob']

chain_configs


# In[4]:


# List of chains
# chains = get_chain_names_from_df(chain_configs)

# Start date for backfilling
# start_date = datetime.date(2021, 11, 1)
start_date = datetime.date(2024, 5, 1)
end_date = datetime.date.today() + datetime.timedelta(days=1)

print(end_date)


# In[5]:


def get_query_from_file(mv_name):
    try:
        # Try to get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # If __file__ is not defined (e.g., in Jupyter), use the current working directory
        script_dir = os.getcwd()
    
    query_file_path = os.path.join(script_dir, 'mv_inputs', f'{mv_name}.sql')
    # print(f"Attempting to read query from: {query_file_path}")
    
    try:
        with open(query_file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"Error: Query file not found: {query_file_path}")
        raise


# In[6]:


def set_optimize_on_insert(option_int = 1):
    client.command(f"""
        SET optimize_on_insert = {option_int};
        """)
    print(f"Set optimize_on_insert = {option_int}")


# In[ ]:





# In[7]:


import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError

def create_materialized_view(client, chain, mv_name, block_time = 2):
    table_view_name = f'{chain}_{mv_name}'
    full_view_name = f'{chain}_{mv_name}_mv'
    create_file_name = f'{mv_name}_create'
    print(full_view_name)
    
    # Check if create file exists
    if not os.path.exists(f'mv_inputs/{create_file_name}.sql'):
        print(f"Table create file {create_file_name}.sql does not exist. Skipping table creation.")
    else:
        try:
            # Check if table already exists
            result = client.query(f"SHOW TABLES LIKE '{table_view_name}'")
            result_rows = list(result.result_rows)
            if result_rows:
                print(f"Table {table_view_name} already exists. Skipping creation.")
            else:
                # Create the table
                create_query = get_query_from_file(create_file_name)
                create_query = create_query.format(chain=chain, view_name=table_view_name)
                client.command(create_query)
                print(f"Created table {table_view_name}")
        except ClickHouseError as e:
            print(f"Error creating table {table_view_name}: {str(e)}")
            return  # Exit the function if table creation fails

    try:
        # Check if view already exists
        result = client.query(f"SHOW TABLES LIKE '{full_view_name}'")
        result_rows = list(result.result_rows)
        if result_rows:
            print(f"Materialized view {full_view_name} already exists. Skipping creation.")
            return

        query_template = get_query_from_file(f'{mv_name}_mv')
        query = query_template.format(chain=chain, view_name=full_view_name, table_name=table_view_name, block_time_sec=block_time)
        query = gsb.process_goldsky_sql(query)
        # print(query)
        client.command(query)
        print(f"Created materialized view {full_view_name}")
    except ClickHouseError as e:
        print(f"Error creating materialized view {full_view_name}: {str(e)}")


def ensure_backfill_tracking_table_exists(client):
    check_table_query = """
    SELECT 1 FROM system.tables 
    WHERE database = currentDatabase() AND name = 'backfill_tracking'
    """
    result = client.query(check_table_query)
    
    if not result.result_rows:
        create_table_query = """
        CREATE TABLE backfill_tracking (
            chain String,
            mv_name String,
            start_date Date,
            end_date Date
        ) ENGINE = MergeTree()
        ORDER BY (chain, mv_name, start_date)
        """
        client.command(create_table_query)
        print("Created backfill_tracking table.")
    else:
        print("backfill_tracking table already exists.")

def backfill_data(client, chain, mv_name, end_date = end_date, block_time = 2):
    full_view_name = f'{chain}_{mv_name}_mv'
    full_table_name = f'{chain}_{mv_name}'
    current_date_q = f"SELECT DATE_TRUNC('day',MIN(timestamp)) AS start_dt FROM {chain}_blocks WHERE number = 1 AND is_deleted = 0"
    current_date = client.query(current_date_q).result_rows[0][0].date()

    while current_date <= end_date:
        print(f"{chain} - {mv_name}: Current date: {current_date} - End Date: {end_date}")
        attempts = 1
        is_success = 0
        days_batch_size = set_days_batch_size
        
        while (is_success == 0) & (attempts < 3) & (current_date + datetime.timedelta(days=days_batch_size) <= end_date):
            batch_size = datetime.timedelta(days=days_batch_size)
            print(f"attempt: {attempts}")
            batch_end = min(current_date + batch_size, end_date)
            # print('checking backfill tracking')
            # Check if this range has been backfilled
            check_query = f"""
            SELECT MAX(start_date) AS latest_fill_start
            FROM backfill_tracking
            WHERE chain = '{chain}'
            AND mv_name = '{mv_name}'
            HAVING 
                MIN(start_date) <= toDate('{current_date}')
            AND MAX(end_date) >= toDate('{batch_end}')
            LIMIT 1
            """
            result = client.query(check_query)

            if result.result_rows: # Get date to start backfilling
                latest_fill_start = result.result_rows[0][0]
                # print(f"Latest Fill Result: {latest_fill_start}")
                current_date = max(latest_fill_start, current_date)
                batch_end = min(current_date + batch_size, end_date)
            else:
                print("no backfill exists")
            # print(f"Fill start: {current_date}")

            # print(check_query)
            # print(result.result_rows)
            #Check if data already exists


            if not result.result_rows:
                # No record of backfill, proceed
                query_template = get_query_from_file(f'{mv_name}_backfill')
                query = query_template.format(
                    view_name=full_view_name,
                    chain=chain,
                    start_date=current_date,
                    end_date=batch_end,
                    table_name = full_table_name,
                    block_time_sec = block_time
                )
                query = gsb.process_goldsky_sql(query)
                
                # print(query)
                try:
                    # print(query)
                    # set_optimize_on_insert(0) # for runtime
                    print(f"Starting backfill for {full_view_name} from {current_date} to {batch_end}")
                    client.command(query)
                    # Record the backfill
                    track_query = f"""
                    INSERT INTO backfill_tracking (chain, mv_name, start_date, end_date)
                    VALUES ('{chain}', '{mv_name}', toDate('{current_date}'), toDate('{batch_end}'))
                    """
                    client.command(track_query)
                    
                    print(f"Backfilled data for {full_view_name} from {current_date} to {batch_end}")

                    # Optimize the newly backfilled partition
                    # optimize_partition(client, full_view_name, current_date, batch_end)
                    is_success = 1
                except Exception as e:
                    print(f"Error during backfill for {full_view_name} from {current_date} to {batch_end}: {str(e)}")
                    days_batch_size = 1
                    attempts += 1
                time.sleep(1)
            else:
                print(f"Data already backfilled for {full_view_name} from {current_date} to {batch_end}. Skipping.")
                is_success = 1
                # if optimize_all:
                #     optimize_partition(client, full_view_name, current_date, batch_end)
        # print(f"Current Date: {current_date}, Batch End: {batch_end}")
        current_date = max(batch_end,current_date) + datetime.timedelta(days=1)
        # print(f"New Current Date: {current_date}")

# def optimize_partition(client, full_view_name, start_date, end_date):
#     # First, let's get the actual partition names
#     partition_query = f"""
#     SELECT DISTINCT partition
#     FROM system.parts
#     WHERE table = '{full_view_name.split('.')[-1]}'
#       AND database = '{full_view_name.split('.')[0]}'
#       AND active
#     ORDER BY partition
#     """
#     print(partition_query)
#     partitions = [row[0] for row in client.query(partition_query).result_rows]
    
#     print(f"Available partitions for {full_view_name}: {partitions}")

#     # Filter partitions within our date range
#     start_partition = start_date.strftime('%Y%m')
#     end_partition = end_date.strftime('%Y%m')
#     partitions_to_optimize = [p for p in partitions if start_partition <= p <= end_partition]

#     if not partitions_to_optimize:
#         print(f"No partitions found for {full_view_name} between {start_date} and {end_date}")
#         return

#     try:
#         for partition in partitions_to_optimize:
#             optimize_query = f"""
#             OPTIMIZE TABLE {full_view_name} 
#             PARTITION '{partition}'
#             FINAL SETTINGS max_execution_time = 3000
#             """
#             print(f"Attempting to optimize {full_view_name} for partition {partition}")
#             client.command(optimize_query)
#             print(f"Successfully optimized partition {partition} for {full_view_name}")
#     except Exception as e:
#         print(f"Error executing OPTIMIZE TABLE for {full_view_name}")
#         print(f"  Partition: {partition}")
#         print(f"  Date range: {start_date} to {end_date}")
#         print(f"  Error: {str(e)}")


# In[8]:


def reset_materialized_view(client, chain, mv_name, block_time = 2):
    full_view_name = f'{chain}_{mv_name}_mv'
    table_name = f'{chain}_{mv_name}'

    try:
        # Drop the existing materialized view
        client.command(f"DROP TABLE IF EXISTS {full_view_name}")
        # client.command(f"DROP MATERIALIZED VIEW IF EXISTS {full_view_name}")
        print(f"Dropped materialized view {full_view_name}")

        # Drop the existing materialized view
        client.command(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped table {table_name}")

        # # Recreate the materialized view using the existing function
        # create_materialized_view(client, chain, mv_name, block_time)
        # print(f"Recreated materialized view {full_view_name}")

        # Clear the backfill tracking for this view
        client.command(f"""
        ALTER TABLE backfill_tracking 
        DELETE WHERE chain = '{chain}' AND mv_name = '{mv_name}'
        """)
        print(f"Cleared backfill tracking for {full_view_name}")

    except Exception as e:
        print(f"Error resetting materialized view {full_view_name}: {str(e)}")


# In[10]:


# # # # # To reset a view
# for row in chain_configs.itertuples(index=False):
#         chain = row.chain_name
#         reset_materialized_view(client, chain, 'across_bridging_txs_v3', 2)

#reset a single chain
# reset_materialized_view(client, 'zora', 'across_bridging_txs_v3', 2)

# # # # # # # for mv in mv_names:
# # # # # # #         # print(row)
# # # # # # #         reset_materialized_view(client, 'bob', mv, 2)


# # # Clear all
# # # mv_names
# # # for row in chain_configs.itertuples(index=False):
# # #         for mv in mv_names:
# # #                 chain = row.chain_name
# # #                 reset_materialized_view(client, chain, mv, 2)


# In[11]:


# Main execution
ensure_backfill_tracking_table_exists(client)

for row in chain_configs.itertuples(index=False):

    chain = row.chain_name
    block_time = row.block_time_sec
    print(f"Processing chain: {chain}")
    for mv_name in mv_names:
        
        try:
            print('create matview')
            create_materialized_view(client, chain, mv_name, block_time = block_time)
        except:
            print('error')
        try:
            print('create backfill')
            backfill_data(client, chain, mv_name, end_date = end_date, block_time = block_time)
        except Exception as e:
            print('An error occurred:')
            print(str(e))
            print('Traceback:')
            print(traceback.format_exc())
        
    print(f"Completed processing for {chain}")

print("All chains and views processed successfully")

