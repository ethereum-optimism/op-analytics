import pandas as pd
import numpy as np

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
        md = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')
        return md

def get_op_stack_metadata_by_data_source(data_source, col_rename = 'blockchain'):
        if data_source == 'oplabs':
                col = 'oplabs_db_schema'
        elif data_source == 'flipside':
                col = 'flipside_schema'
        elif data_source == 'dune':
                col = 'dune_schema'
        
        col_list = ['chain_name','display_name','mainnet_chain_id','chain_layer','alignment'] + [col]
        
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
        chain_ids = df_col.astype(int).astype(str).tolist()
        chain_ids_string = ','.join(chain_ids)
        return chain_ids_string

def get_unique_chain_ids_from_dfs(dataframes):
        unique_chain_list = set()
        for df in dataframes:
                if 'chain_id' in df.columns:
                        # Convert to float to handle decimal numbers, then to set to get unique values
                        chain_ids = set(df['chain_id'].astype(int).astype(str).dropna())
                        unique_chain_list.update(chain_ids)
                else:
                        print(f"Warning: 'chain_id' column not found in one of the dataframes.")
                chain_ids_string = ','.join(unique_chain_list)

        print(chain_ids_string)
        return chain_ids_string
                