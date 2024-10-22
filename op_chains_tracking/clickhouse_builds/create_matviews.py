#!/usr/bin/env python
# coding: utf-8

# In[1]:


# List of materialized view names
mvs = [
    {'mv_name': 'across_bridging_txs_v3', 'start_date': '2024-07-01', 'chains': ''},
    # # {'mv_name': 'across_bridging_txs_v3_logs_only', 'start_date': '2024-07-01'},
    # # {'mv_name': 'filtered_logs_l2s', 'start_date': ''},
    # ### {'mv_name': 'erc20_transfers', 'start_date': ''},
    # ### {'mv_name': 'native_eth_transfers', 'start_date': ''},
    # ### {'mv_name': 'transactions_unique', 'start_date': ''},
    # # {'mv_name': 'daily_aggregate_transactions_to', 'start_date': ''},
    # {'mv_name': 'event_emitting_transactions_l2s', 'start_date': ''},
    {'mv_name': 'weekly_retention_rate_temp', 'start_date': '2024-01-01', 'chains': 'superchain'},
    {'mv_name': 'event_emitting_transactions_l2s_nofilter', 'start_date': '2024-01-01', 'chains': ''},
    # {'mv_name': 'event_emitting_transactions_l2s_nofilter', 'start_date': '2024-01-01'},
    
    
]

set_days_batch_size = 30# 7 #30

optimize_all = False #True


# In[2]:


topic0_maps = [
        {'table_type': 'materialized', 'topic0_func': "topic0"},
        {'table_type': 'raw', 'topic0_func': "arrayElement(splitByString(',', topics), 1)"}
]


# In[ ]:


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


# In[4]:


mv_names = [item['mv_name'] for item in mvs]

# Get Chain List
chain_configs = ops.get_superchain_metadata_by_data_source('oplabs') # OPLabs db

if client is None:
        client = ch.connect_to_clickhouse_db()

# Function to create ClickHouse view
def get_chain_names_from_df(df):
    return df['blockchain'].dropna().unique().tolist()

# chain_configs = chain_configs[chain_configs['chain_name'] == 'xterio']


# In[ ]:


# List of chains
# chains = get_chain_names_from_df(chain_configs)

# Start date for backfilling
start_date = datetime.date(2021, 11, 1)
# start_date = datetime.date(2024, 5, 1)
end_date = datetime.date.today() #+ datetime.timedelta(days=1)

print(end_date)


# In[6]:


og_start_date = start_date
og_end_date = end_date


# In[7]:


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


# In[8]:


def set_optimize_on_insert(option_int = 1):
    client.command(f"""
        SET optimize_on_insert = {option_int};
        """)
    print(f"Set optimize_on_insert = {option_int}")

def do_optimize_final(client, chain_name, mv_name):
    # print(f'Optimizing: {chain_name}_{mv_name}')
    opcmd = f'OPTIMIZE TABLE {chain_name}_{mv_name} FINAL;'
    print(opcmd)
    client.command(opcmd)


# In[9]:


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

    # Comment out matview since we're backfilling daily now
    # try:
    #     # Check if view already exists
    #     result = client.query(f"SHOW TABLES LIKE '{full_view_name}'")
    #     result_rows = list(result.result_rows)
    #     if result_rows:
    #         print(f"Materialized view {full_view_name} already exists. Skipping creation.")
    #         return

    #     query_template = get_query_from_file(f'{mv_name}_mv')
    #     query = query_template.format(chain=chain, view_name=full_view_name, table_name=table_view_name, block_time_sec=block_time)
    #     query = gsb.process_goldsky_sql(query)
    #     # Save the query
    #     output_folder = os.path.join("mv_outputs", "sql")
    #     os.makedirs(output_folder, exist_ok=True)
    #     filename = f"{mv_name}_mv.sql"
    #     file_path = os.path.join(output_folder, filename)
    #     with open(file_path, 'w') as file:
    #         file.write(query)
    #     # print(query)
    #     client.command(query)
    #     print(f"Created materialized view {full_view_name}")
    # except ClickHouseError as e:
    #     print(f"Error creating materialized view {full_view_name}: {str(e)}")


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


# In[10]:


# LEGACY FUNCTIONS
# def backfill_data(client, chain, mv_name, end_date = end_date, block_time = 2, mod_start_date = start_date):
#     full_view_name = f'{chain}_{mv_name}_mv'
#     full_table_name = f'{chain}_{mv_name}'
#     if mod_start_date == '':
#         current_date_q = f"SELECT DATE_TRUNC('day',MIN(timestamp)) AS start_dt FROM {chain}_blocks WHERE number = 1 AND is_deleted = 0"
#         current_date = client.query(current_date_q).result_rows[0][0].date()
#     else:
#         current_date = pd.to_datetime(mod_start_date).date()

#     while current_date <= end_date:
#         print(f"{chain} - {mv_name}: Current date: {current_date} - End Date: {end_date}")
#         attempts = 1
#         is_success = 0
#         days_batch_size = set_days_batch_size
        
#         while (is_success == 0) & (attempts <= 3) :#& (current_date + datetime.timedelta(days=days_batch_size) <= end_date):
#             if attempts == 1:
#                 days_batch_size = set_days_batch_size
#             elif attempts == 2:
#                 days_batch_size = int( set_days_batch_size / 2 )
#                 print(f'reset batch size to {days_batch_size}')
#             else:
#                 days_batch_size = 1
#                 print(f'reset batch size to {days_batch_size}')
            
#             batch_size = datetime.timedelta(days=days_batch_size)
#             print(f"attempt: {attempts}")
#             batch_end = min(current_date + batch_size, end_date)
#             # print(f'init batch end: {batch_end}')
#             # print('checking backfill tracking')
#             # Check if this range has been backfilled
#             check_query_temp = get_query_from_file('backfill_check_query')
#             check_query = check_query_temp.format(
#                     mv_name=mv_name,
#                     chain=chain,
#                     start_date=current_date,
#                     end_date=batch_end
#                 )
#             result = client.query(check_query)

#             if result.result_rows: # Get date to start backfilling
#                 latest_fill_start = result.result_rows[0][0]
#                 # print(f"Latest Fill Result: {latest_fill_start}")
#                 current_date = max(latest_fill_start, current_date)
#                 batch_end = min(current_date + batch_size, end_date + datetime.timedelta(days = 1))
#             else:
#                 print("no backfill exists")
            
#             # print(f'backfill check batch end: {batch_end}')
#             # print(f"Fill start: {current_date}")

#             # print(check_query)
#             # print(result.result_rows)
#             #Check if data already exists

#             # Start 1 day back
#             query_start_date = current_date - datetime.timedelta(days = 1)
#             query_end_date = batch_end #+ datetime.timedelta(days = 1)


#             if not result.result_rows:
#                 # No record of backfill, proceed
#                 query_template = get_query_from_file(f'{mv_name}_backfill')
#                 query = query_template.format(
#                     view_name=full_view_name,
#                     chain=chain,
#                     start_date=query_start_date,
#                     end_date=query_end_date,
#                     table_name = full_table_name,
#                     block_time_sec = block_time
#                 )
#                 query = gsb.process_goldsky_sql(query)
                
#                 # print(query)
#                 try:
#                     # print(query)
#                     # set_optimize_on_insert(0) # for runtime
#                     print(f"Starting backfill for {full_view_name} from {query_start_date} to {batch_end}")

#                     # # Save the query
#                     # output_folder = os.path.join("mv_outputs", "sql")
#                     # os.makedirs(output_folder, exist_ok=True)
#                     # filename = f"{chain}_{mv_name}_backfill.sql"
#                     # file_path = os.path.join(output_folder, filename)
#                     # with open(file_path, 'w') as file:
#                     #     file.write(query)

#                     client.command(query)
#                     # Record the backfill
#                     track_query = f"""
#                     INSERT INTO backfill_tracking (chain, mv_name, start_date, end_date)
#                     VALUES ('{chain}', '{mv_name}', toDate('{current_date}'), toDate('{batch_end}'))
#                     """
#                     client.command(track_query)
                    
#                     print(f"Backfilled data for {full_view_name} from {query_start_date} to {batch_end}")

#                     # Optimize the newly backfilled partition
#                     # optimize_partition(client, full_view_name, current_date, batch_end)
#                     is_success = 1
#                 except Exception as e:
#                     print(f"Error during backfill for {full_view_name} from {current_date} to {batch_end}: {str(e)}")
#                     attempts += 1
#                 time.sleep(1)
#             else:
#                 print(f"Data already backfilled for {full_view_name} from {current_date} to {batch_end}. Skipping.")
#                 is_success = 1
#                 # if optimize_all:
#                 #     optimize_partition(client, full_view_name, current_date, batch_end)
#         # print(f"Current Date: {current_date}, Batch End: {batch_end}")
#         current_date = max(batch_end,current_date + datetime.timedelta(days=1))

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


# In[11]:


def backfill_data(client, chain, mv_name, end_date, block_time=2, mod_start_date='', set_days_batch_size=7):
    full_view_name = f'{chain}_{mv_name}_mv'
    full_table_name = f'{chain}_{mv_name}'

    if chain in ['op', 'base']:
        topic0_func = next(item['topic0_func'] for item in topic0_maps if item['table_type'] == 'materialized')
    else:
        topic0_func = next(item['topic0_func'] for item in topic0_maps if item['table_type'] == 'raw')
    
    # Check on Date Ranges
    current_date_q = f"""
    SELECT 
        DATE_TRUNC('day', MIN(timestamp)) AS start_dt, 
        DATE_TRUNC('day', MAX(timestamp)) AS end_dt 
    FROM {chain}_blocks 
    WHERE number >= 1 AND is_deleted = 0
    """
    result = client.query(current_date_q).result_rows
    
    if result and result[0][0] is not None and result[0][1] is not None:
        start_date_result = max(result[0][0].date() , og_start_date) # handle for corrupt timestamp
        end_date_result = min( result[0][1].date(), og_end_date) # handle for corrupt timestamp
        print(f'start_date_result {start_date_result} - end_date_result {end_date_result}')
    else:
        start_date_result = start_date
        end_date_result = end_date

    # Determine start date
    if mod_start_date == '':
        start_date = start_date_result
    else:
        start_date = pd.to_datetime(mod_start_date).date()

    start_date = start_date - datetime.timedelta(days=1) #ensure we fill in prior day

    # Determine end date
    end_date = end_date_result

    # Print out resulting range
    print(f"Checking Backfill for {full_view_name} from {start_date} to {end_date}")

    # Fetch all existing backfill ranges
    backfill_query = f"""
    SELECT start_date, end_date
    FROM backfill_tracking
    WHERE chain = '{chain}' AND mv_name = '{mv_name}'
    AND start_date >= toDate('{start_date}')
    ORDER BY start_date
    """
    backfill_ranges = client.query(backfill_query).result_rows

    # Convert to list of tuples and sort
    backfill_ranges = sorted([(row[0], row[1]) for row in backfill_ranges])

    current_date = start_date
    while current_date <= end_date:
        # Find the next date that needs backfilling
        while current_date <= end_date:
            # print(f'check: start {start_date}, current {current_date}, end {end_date}')
            is_backfilled = any(bf_start <= current_date <= bf_end for bf_start, bf_end in backfill_ranges)
            if not is_backfilled:
                break
            current_date += datetime.timedelta(days=1)
        
        if current_date > end_date:
            print(f"All dates up to {end_date} have been backfilled.")
            break

        # Determine the batch end date
        batch_end = min(current_date + datetime.timedelta(days=set_days_batch_size - 1), end_date)
        
        # Adjust batch_end if it overlaps with an existing backfill range
        for bf_start, bf_end in backfill_ranges:
            if current_date < bf_start <= batch_end:
                batch_end = bf_start - datetime.timedelta(days=1)
                break

        # Perform the backfill
        attempts = 1
        query_start_date = current_date - datetime.timedelta(days=1)

        while attempts <= 3:
            try:
                query_template = get_query_from_file(f'{mv_name}_backfill')
                query = query_template.format(
                    view_name=full_view_name,
                    chain=chain,
                    start_date=query_start_date,
                    end_date=batch_end,
                    table_name=full_table_name,
                    block_time_sec=block_time,
                    topic0_func=topic0_func
                )
                query = gsb.process_goldsky_sql(query)
                
                print(f"Starting backfill for {full_view_name} from {query_start_date} to {batch_end}")
                client.command(query)
                
                # Record the backfill
                track_query = f"""
                INSERT INTO backfill_tracking (chain, mv_name, start_date, end_date)
                VALUES ('{chain}', '{mv_name}', toDate('{query_start_date}'), toDate('{batch_end}'))
                """
                client.command(track_query)
                
                print(f"Backfilled data for {full_view_name} from {query_start_date} to {batch_end}")
                
                # Update backfill_ranges with the new range
                backfill_ranges.append((current_date, batch_end))
                backfill_ranges.sort()
                
                break
            except Exception as e:
                print(f"Error during backfill for {full_view_name} from {current_date} to {batch_end}: {str(e)}")
                attempts += 1
                batch_end = min(current_date + datetime.timedelta(days=set_days_batch_size // (2 ** (attempts - 1))), end_date)
            time.sleep(1)

        current_date = batch_end + datetime.timedelta(days=1)


# In[12]:


def detach_reset_materialized_view(client, chain, mv_name):
    table_view_name = f'{chain}_{mv_name}'
    full_view_name = f'{chain}_{mv_name}_mv'
    create_file_name = f'{mv_name}_create'
    print(full_view_name)
    query_template = get_query_from_file(f'{mv_name}_mv')
    query = query_template.format(chain=chain, view_name=full_view_name, table_name=table_view_name, block_time_sec=block_time)
    query = gsb.process_goldsky_sql(query)
    client.command(query)
    print(f"Updated materialized view {full_view_name}")

    # dt_cmd = f"DETACH TABLE PERMANENTLY {full_view_name}"
    # dt_cmd = f"ALTER TABLE {full_view_name} MODIFY QUERY SELECT 1 WHERE 0"

    # print(dt_cmd)
    client.command(dt_cmd)
    print(f"Detached table {full_view_name}")

def reset_materialized_view(client, chain, mv_name,):
    full_view_name = f'{chain}_{mv_name}_mv'
    table_name = f'{chain}_{mv_name}'

    try:
        # Drop the existing materialized view
        client.command(f"DROP TABLE IF EXISTS {full_view_name}")
        # client.command(f"DROP MATERIALIZED VIEW IF EXISTS {full_view_name}")
        print(f"Dropped materialized view {full_view_name}")

        # Drop the existing materialized view
        client.command(f"DROP TABLE IF EXISTS {table_name}")
        client.command(f"DROP TABLE IF EXISTS {full_view_name}")
        print(f"Dropped table {table_name}")

        # Clear the backfill tracking for this view
        bf_delete = f"""
        ALTER TABLE backfill_tracking 
        DELETE WHERE chain = '{chain}' AND mv_name = '{mv_name}'
        """
        # print(bf_delete)
        client.command(bf_delete)

        print(f"Cleared backfill tracking for {full_view_name}")

    except Exception as e:
        print(f"Error resetting materialized view {full_view_name}: {str(e)}")


# In[13]:


def print_backfill_gaps(client):
    query = get_query_from_file('find_backfill_gaps')
    result = client.query(query)

    if result.result_rows:
        print("Backfill gaps found:")
        print("Chain | Table/View Name | Gap Start | Gap End")
        print("-" * 50)
        for row in result.result_rows:
            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}")
    else:
        print("No backfill gaps found.")


# In[14]:


# # # # To reset a view
# for row in chain_configs.itertuples(index=False):
#         chain = row.chain_name
#         reset_materialized_view(client, chain, 'weekly_retention_rate_temp')

# # # # # # # reset a single chain
# # # # # reset_materialized_view(client, 'xterio', 'daily_aggregate_transactions_to')

# # # # # # # # # # # # for mv in mv_names:
# # # # # # # # # # # #         # print(row)
# # # # # # # # # # # #         reset_materialized_view(client, 'bob', mv)


# # # # # # # # Clear all
# # # # # # # # mv_names
# # # # # # # # for row in chain_configs.itertuples(index=False):
# # # # # # # #         for mv in mv_names:
# # # # # # # #                 chain = row.chain_name
# # # # # # # #                 reset_materialized_view(client, chain, mv)

# # # Detach all
# # detach_reset_materialized_view
# # for row in chain_configs.itertuples(index=False):
# #         for mv in mv_names:
# #                 chain = row.chain_name
# #                 detach_reset_materialized_view(client, chain, mv)


# In[15]:


# # Optimize
# for row in chain_configs.itertuples(index=False):
#         for mv in mv_names:
#                 chain = row.chain_name
#                 do_optimize_final(client, chain, mv)


# In[16]:


og_chain_configs = chain_configs
chain_configs_if_agg = pd.DataFrame({
    'chain_name': ['superchain'],
    'block_time_sec': [2]
})


# In[17]:


# chain_configs_if_agg


# In[ ]:


# Main execution
ensure_backfill_tracking_table_exists(client)

for mv_row in mvs:

    if mv_row['chains'] == 'superchain':
        chain_configs = chain_configs_if_agg
    else:
        chain_configs = og_chain_configs
        
    for chain_row in chain_configs.itertuples(index=False):
        chain = chain_row.chain_name
        block_time = chain_row.block_time_sec

        mv_name = mv_row['mv_name']
        print(f"Processing chain: {chain} - {mv_name}")

        if mv_row['start_date'] != '':
            mod_start_date = mv_row['start_date']
        else:
            mod_start_date = ''
        
        try:
            print('create matview')
            create_materialized_view(client, chain, mv_name, block_time = block_time)
        except:
            print('error')
        try:
            print('create backfill')
            backfill_data(client, chain, mv_name, end_date = end_date, block_time = block_time, mod_start_date = mod_start_date, set_days_batch_size = set_days_batch_size)
        except Exception as e:
            print('An error occurred:')
            print(str(e))
            print('Traceback:')
            print(traceback.format_exc())
        
    print(f"Completed processing for {chain}")

print("All chains and views processed successfully")


# In[ ]:


print_backfill_gaps(client)

