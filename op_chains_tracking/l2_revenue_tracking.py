#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ! pip install --upgrade dune-client
# ! pip show dune-client


# In[ ]:


# Get L2 Revenue and post it to a database (csv in github for now)
# TODO - Integrate with BQ upload
import pandas as pd
import sys
import time
import json
sys.path.append("../helper_functions")
# import web3py_utils as w3py
# import duneapi_utils as du
import google_bq_utils as bqu
from web3 import Web3
from datetime import datetime, timezone
sys.path.pop()

import os


# In[ ]:


# https://github.com/ethereum-optimism/optimism/blob/b86522036ad11a91de1d1dadb6805167add83326/specs/predeploys.md?plain=1#L50

# Name, contract
fee_vaults = [
    ['SequencerFeeVault','0x4200000000000000000000000000000000000011'],
    ['BaseFeeVault','0x4200000000000000000000000000000000000019'],
    ['L1FeeVault','0x420000000000000000000000000000000000001A'],
]

# Aiming to eventually read from superchain-resitry + some non-superchain static adds
chains_rpcs = pd.read_csv('outputs/chain_metadata.csv', na_filter=False)
# print(chains_rpcs.columns)
chains_rpcs = chains_rpcs[~(chains_rpcs['rpc_url'] == '') & ~(chains_rpcs['op_based_version'].str.contains('legacy'))]
# chains_rpcs = chains_rpcs.values.tolist()
# print(chains_rpcs.sample(5))

# # Temp
# chains_rpcs = chains_rpcs.head(1)
# chains_rpcs


# In[ ]:


# Calculate the method signature hash
method_signature = "totalProcessed()"
method_id = Web3.keccak(text=method_signature)[:4].hex()
# Verify the method ID
print(f"Method ID: {method_id}")


# In[ ]:


df_columns = [
    'block_time', 'block_number', 'chain_name', 'vault_name', 
    'vault_address', 'alltime_revenue_native', 'chain_id'
    # 'gas_token', 'da_layer', 'is_superchain_registry' # Uncomment these if needed
]


# In[ ]:


data_arr = []

for index, chain in chains_rpcs.iterrows():
    chain_name = chain['chain_name']
    chain_id = chain['mainnet_chain_id']

    # if chain_id:  # Check if chain_id is not empty
    #     chain_id = int(float(chain_id))  # Convert to float first, then to int
    # else:
    #     chain_id = None  # or keep it as an empty string or any default value you prefer
        
    print(chain_name + ' - ' + str(chain_id))
    rpc = chain['rpc_url']
    # gas_token = chain['gas_token']
    # da_layer = chain['da_layer']
    # is_superchain_registry = chain['superchain_registry']

    try:
        w3_conn = Web3(Web3.HTTPProvider(rpc))
        # Get the timestamp of the latest block
        block_timestamp = w3_conn.eth.get_block('latest').timestamp
        block_number = w3_conn.eth.get_block('latest').number
        # Convert the UNIX timestamp to a human-readable format
        block_datetime = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)
        block_time = block_datetime.strftime('%Y-%m-%d %H:%M:%S')

        for vault in fee_vaults:
            vault_name = vault[0]
            vault_address = vault[1]
            # Call the function directly using eth_call
            response = w3_conn.eth.call({
                'to': vault_address,
                'data': method_id
            })
            wei_balance = w3_conn.eth.get_balance(vault_address)
            # Decode the result (assuming the function returns a uint256)
            proxy_processed_wei = Web3.to_int(hexstr=response.hex())
            
            alltime_revenue_wei = proxy_processed_wei+wei_balance
            alltime_revenue_native = alltime_revenue_wei/1e18

            print(chain_name + ' | ' + vault_name + ': ' \
                + str(proxy_processed_wei) + ' | bal: ' + str(wei_balance)\
                + ' | total eth: ' + str( (alltime_revenue_native) )
                )
            
            tmp = pd.DataFrame(
                    [[block_time, block_number, chain_name, vault_name, vault_address, alltime_revenue_native, chain_id]]#, gas_token, da_layer, is_superchain_registry]]
                    ,columns =df_columns
                    )
            data_arr.append(tmp)
            time.sleep(1)
    except:
        continue

data_df = pd.concat(data_arr)
data_df['block_time'] = pd.to_datetime(data_df['block_time'])


# In[ ]:


file_path = 'outputs/all_time_revenue_data.csv'

data_df.sample(3)


# In[ ]:


# Check if the file exists
if os.path.exists(file_path):
    # Read the existing CSV file
    existing_df = pd.read_csv(file_path)
    # Ensure the existing file has the same columns
    existing_df = existing_df.reindex(columns=df_columns)
    data_df = data_df.reindex(columns=df_columns)
    # Append without writing the header
    data_df.to_csv(file_path, mode='a', header=False, index=False)
else:
    # If file doesn't exist, create it and write the header
    data_df.to_csv(file_path, mode='w', header=True, index=False)


# In[ ]:


# # Overwrite to Dune Table
dune_df = pd.read_csv(file_path)
# print(dune_df.sample(5))
# du.write_dune_api_from_pandas(dune_df, 'op_stack_chains_cumulative_revenue_snapshots',\
#                              'Snapshots of All-Time (cumulative) revenue for fee vaults on OP Stack Chains. Pulled from RPCs - metadata in op_stack_chains_chain_rpc_metdata')


# In[ ]:


# #Insert Updates to Dune Table
# create_namespace = 'oplabspbc'
# create_table_name = 'op_stack_chains_cumulative_revenue_snapshots'
# create_table_description = 'Snapshots of All-Time (cumulative) revenue for fee vaults on OP Stack Chains. Pulled from RPCs - metadata in op_stack_chains_chain_rpc_metdata'
# # try:
# # du.create_dune_table(data_df, namespace = create_namespace
# #                         , table_name = create_table_name
# #                         , table_description = create_table_description)

# # except:
#         # print('error creating')
# du.insert_dune_api_from_pandas(data_df, namespace = create_namespace,table_name = create_table_name)

# du.write_dune_api_from_pandas(chains_rpcs, 'op_stack_chains_chain_rpc_metdata',\
#                              'Chain metadata - used to join with op_stack_chains_cumulative_revenue_snapshots')


# In[ ]:


bq_cols = ['block_time','block_number','chain_name','vault_name','vault_address','alltime_revenue_native','chain_id']


# In[ ]:


# dune_df.sample(5)


# In[ ]:


data_df['chain_id'] = data_df['chain_id'].astype(float)
data_df['block_time'] = pd.to_datetime(data_df['block_time'])
# dune_df['block_time'] = pd.to_datetime(dune_df['block_time'])
data_df.dtypes


# In[ ]:


dataset_name = 'rpc_table_uploads'
table_name = 'hourly_cumulative_l2_revenue_snapshots'

# Write All to BQ Table
# bqu.write_df_to_bq_table(df = dune_df[bq_cols], table_id = table_name, dataset_id = dataset_name)

# Write Updates to BQ Table
unique_cols = ['block_time', 'chain_name', 'chain_id', 'vault_name']
bqu.write_df_to_bq_table(df = data_df[bq_cols], table_id = table_name, dataset_id = dataset_name, write_mode='append')

