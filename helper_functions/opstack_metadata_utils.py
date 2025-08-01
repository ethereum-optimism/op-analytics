import pandas as pd
import numpy as np
import os

def generate_alignment_column(df):

        # Create 'is_op_chain' column
        df['is_op_chain'] = df['chain_type'].notnull().astype(bool)

        conditions = [
        df['is_op_chain'] == True,
        (df['is_op_chain'] == False),
        df['is_op_chain'].isna()
        ]

        choices = [
        'OP Chain',
        'OP Stack fork',
        'Other EVMs'
        ]


        df['alignment'] = np.select(conditions, choices, default='Unknown')
        # Add 'Legacy ' prefix to 'alignment' if 'op_based_version' contains 'Legacy'
        df['alignment'] = df.apply(lambda row: 'Legacy ' + row['alignment'] 
                                if pd.notna(row['op_based_version']) and 'Legacy' in row['op_based_version'] 
                                else row['alignment'], axis=1)

        return df


def get_op_stack_metadata_df():
    # Find the root directory (where 'op-analytics' is located)
    root_dir = find_root_dir()
    
    # Construct the path to the CSV file
    csv_path = os.path.join(root_dir, 'op_chains_tracking', 'outputs', 'chain_metadata.csv')
    
    # Read and return the DataFrame
    return pd.read_csv(csv_path)

def find_root_dir():
    current_dir = os.path.abspath(os.getcwd())
    while True:
        # Check if we're already in the op-analytics project root by looking for characteristic directories
        if (os.path.exists(os.path.join(current_dir, 'op_chains_tracking')) and 
            os.path.exists(os.path.join(current_dir, 'helper_functions'))):
            return current_dir
        # Check if we are already in a directory named 'op-analytics'
        if os.path.basename(current_dir) == 'op-analytics':
            return current_dir
        # Check if there's an 'op-analytics' subdirectory
        if os.path.exists(os.path.join(current_dir, 'op-analytics')):
            print(current_dir)
            print('adding op-analytics')
            return os.path.join(current_dir, 'op-analytics')
        parent_dir = os.path.dirname(current_dir)

        if parent_dir == current_dir:  # We've reached the root of the file system
            raise FileNotFoundError("Could not find 'op-analytics' directory")
        current_dir = parent_dir

def get_superchain_metadata_by_data_source(data_source, col_rename = 'blockchain'):
        opsup = get_op_stack_metadata_by_data_source(data_source, col_rename)
        opsup = opsup[opsup['alignment']=='OP Chain']
        return opsup

def get_op_stack_metadata_by_data_source(data_source, col_rename = 'blockchain'):
        if data_source == 'oplabs':
                col = 'oplabs_db_schema'
        elif data_source == 'flipside':
                col = 'flipside_schema'
        elif data_source == 'dune':
                col = 'dune_schema'
        
        col_list = ['chain_name','display_name','mainnet_chain_id','chain_layer','alignment','da_layer','output_root_layer','gas_token','block_time_sec','public_mainnet_launch_date','op_chain_start'] + [col]
        
        ops = get_op_stack_metadata_df()
        ops = ops[col_list][~ops[col].isna()]

        ops = ops.rename(columns={col:col_rename})

        return ops


def generate_op_stack_chain_config_query_list(source_order=['oplabs', 'flipside']):
        aggs = []
        for s in source_order:
                cl = get_op_stack_metadata_by_data_source(s)
                cl['source'] = s
                aggs.append(cl)  # Append the DataFrame, not the source string
    
        if aggs:  # Check if aggs is not empty
                fcl = pd.concat(aggs, ignore_index=True)
                # Remove duplicates, keeping the first occurrence (which will be from the earlier source in source_order)
                fcl = fcl.drop_duplicates(subset='mainnet_chain_id', keep='first')
                return fcl
        else:
                print("No data found for the specified sources.")
                return pd.DataFrame()  # Return an empty DataFrame if no data

def gen_chain_ids_list_for_param(df_col):
        chain_ids = df_col.astype(str).tolist()
        chain_ids_string = ','.join(chain_ids)
        return chain_ids_string

def get_unique_chain_ids_from_dfs(dataframes):
        unique_chain_list = set()
        for df in dataframes:
                if 'chain_id' in df.columns:
                        # Convert to float to handle decimal numbers, then to set to get unique values
                        chain_ids = set(df['chain_id'].astype(str).dropna())
                        unique_chain_list.update(chain_ids)
                else:
                        print(f"Warning: 'chain_id' column not found in one of the dataframes.")
                chain_ids_string = ','.join(unique_chain_list)

        print(chain_ids_string)
        return chain_ids_string

def get_rpc_urls_dict():
    """
    Get a dictionary mapping chain names to their RPC URLs from the metadata file.
    Returns a dict where keys are chain names and values are RPC URLs.
    """
    df = get_op_stack_metadata_df()
    # Filter out rows where rpc_url is null or empty
    rpc_df = df[df['rpc_url'].notna() & (df['rpc_url'] != '')]
    
    # Create dictionary mapping chain_name to rpc_url
    rpc_dict = dict(zip(rpc_df['chain_name'], rpc_df['rpc_url']))
    
    return rpc_dict

def get_rpc_url_by_chain_name(chain_name):
    """
    Get RPC URL for a specific chain by its name.
    Returns the RPC URL if found, None otherwise.
    Includes fallback for Ethereum if not found in metadata.
    """
    rpc_dict = get_rpc_urls_dict()
    rpc_url = rpc_dict.get(chain_name)
    
    # Fallback for Ethereum if not found in metadata
    if not rpc_url and chain_name.lower() in ['ethereum', 'eth', 'mainnet']:
        rpc_url = "https://eth.llamarpc.com"
        print(f"Using fallback Ethereum RPC for {chain_name}: {rpc_url}")
    
    return rpc_url
                