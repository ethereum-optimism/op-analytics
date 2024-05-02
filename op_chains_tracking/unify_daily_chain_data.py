#!/usr/bin/env python
# coding: utf-8

# ### Unify CSVs

# In[ ]:


import pandas as pd
import os
print('unify start')


# In[ ]:


opstack_metadata = pd.read_csv('../op_chains_tracking/outputs/chain_metadata.csv')
meta_columns = ['alignment', 'display_name', 'mainnet_chain_id','op_based_version','is_op_chain']
# opstack_metadata


# In[ ]:


# Directory containing your CSV files
directory = 'outputs/chain_data'
output_file = 'outputs/all_chain_activity_by_day_gs.csv'

# Create an empty DataFrame to store combined data
combined_df = pd.DataFrame()

# Loop through the directory and append each matching CSV to the DataFrame
for filename in os.listdir(directory):
    if ('chain_activity_by_day_gs' in filename and filename.endswith('.csv')) & (filename != 'opchain_activity_by_day_gs.csv'):
        print(filename)
        filepath = os.path.join(directory, filename)
        temp_df = pd.read_csv(filepath)
        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
        temp_df = None # Free up Memory

# Save the combined DataFrame to a single CSV file
combined_df['dt'] = pd.to_datetime(combined_df['dt']).dt.strftime('%Y-%m-%d')
combined_df = combined_df.rename(columns={'chain':'chain_gs'})
combined_df.to_csv(output_file, index=False)
print('All CSVs have been combined and saved.')
# print(combined_df.columns)


# In[ ]:


# Join to Defillama
print('start dfl merge')
# Load the TVL data
tvl_data_path = '../other_chains_tracking/outputs/dfl_chain_tvl.csv'
tvl_df = pd.read_csv(tvl_data_path)
tvl_df = tvl_df.rename(columns={'date':'dt','chain':'chain_dfl'})
tvl_df = tvl_df[tvl_df['defillama_slug'].isin(opstack_metadata['defillama_slug'])]
# Remove Partial Days
tvl_df['dt'] = pd.to_datetime(tvl_df['dt']).dt.strftime('%Y-%m-%d')
# tvl_df = tvl_df[tvl_df['dt'].dt.time == pd.Timestamp('00:00:00').time()]
# # Cast to date format
# tvl_df['dt'] = tvl_df['dt'].dt.date
# print(tvl_df.columns)

# Drop Dupe metadata columns
tvl_df = tvl_df.drop(columns=meta_columns, errors='ignore')

# tvl_df


# In[ ]:


print('start l2b merge')
l2b_df = pd.read_csv('../other_chains_tracking/outputs/l2beat_l2_activity.csv')
l2b_df = l2b_df.rename(columns={'timestamp':'dt','chain':'chain_l2b'})
l2b_df['l2beat_slug'] = l2b_df['chain_l2b']
l2b_df = l2b_df.merge(opstack_metadata[['l2beat_slug','chain_name']], on=['l2beat_slug'], how='inner')
l2b_df['dt'] = pd.to_datetime(l2b_df['dt']).dt.strftime('%Y-%m-%d')
l2b_df = l2b_df.drop(columns=meta_columns, errors='ignore')
#Drop NaN
l2b_df.dropna(subset=['valueUsd'], inplace=True)

value_cols = ['valueUsd','cbvUsd','ebvUsd','nmvUsd','valueEth', 'cbvEth', 'ebvEth', 'nmvEth','transactions']

# Update column names and value_cols list
updated_value_cols = []
for col in value_cols:
        new_col_name = col + '_l2b'
        l2b_df.rename(columns={col: new_col_name}, inplace=True)
        updated_value_cols.append(new_col_name)

value_cols = updated_value_cols

l2b_df = l2b_df[['dt','chain_l2b','chain_name'] + value_cols ]


# In[ ]:


print('start dune merge')
dune_df = pd.read_csv('../op_chains_tracking/outputs/l2_transaction_qualification.csv')
#filters
dune_df = dune_df[(dune_df['tx_success'] == True) & (dune_df['is_contract_tx'] == True) & (dune_df['has_event_log'] == True) & (dune_df['has_rev_dev'] == True)]
dune_df = dune_df.rename(columns={'num_raw_transactions':'qualified_transactions','sum_gas_used':'qualified_sum_gas_used'})
dune_df['dt'] = pd.to_datetime(dune_df['dt']).dt.strftime('%Y-%m-%d')

dune_df = dune_df.groupby(['dune_schema','dt',]).agg({
                                'qualified_transactions': 'sum'
                                , 'qualified_sum_gas_used': 'sum'
                                })
dune_df.reset_index(inplace=True)
dune_df = dune_df.merge(opstack_metadata[['dune_schema','chain_name']], on=['dune_schema'], how='inner')

dune_df = dune_df[['dune_schema','chain_name','dt','qualified_transactions','qualified_sum_gas_used']]


# In[ ]:


print(combined_df[['chain_gs','chain_name','dt','num_raw_txs']].sample(3))
print(tvl_df[['chain_dfl','chain_name','dt','tvl']].sample(3))
print(l2b_df[['chain_l2b','chain_name','dt','valueUsd_l2b']].sample(3))
print(dune_df[['dune_schema','chain_name','dt','qualified_transactions']].sample(3))


# In[ ]:


# Perform a left join on 'chain_name' and 'dt'
merged_df = pd.merge(combined_df, tvl_df, on=['chain_name','dt'], how='outer')
merged_df = pd.merge(merged_df, l2b_df, on=['chain_name','dt'], how='outer')
merged_df = pd.merge(merged_df, dune_df, on=['chain_name','dt'], how='outer')

meta_df_columns = meta_columns + ['chain_name']

merged_df = merged_df.merge(opstack_metadata[meta_df_columns], on=['chain_name'], how='left')

# merged_df[merged_df['chain_name']=='zora'].head(5)


# In[ ]:


# Save the merged DataFrame to a new CSV file
output_path = 'outputs/all_chain_data_by_day_combined.csv'
merged_df.to_csv(output_path, index=False)

print('Merged data has been saved to:', output_path)


# ### Import to GSheets

# In[ ]:


# import gspread
# from oauth2client.service_account import ServiceAccountCredentials


# In[ ]:


# # Google Sheets credentials and scope
# scope = ['https://www.googleapis.com/auth/spreadsheets']
# credentials = ServiceAccountCredentials.from_json_keyfile_name('path_to_your_credentials.json', scope)
# client = gspread.authorize(credentials)

# # Open the Google Sheet and select the tab
# sheet = client.open('Name_of_Your_Sheet').worksheet('Transaction Data')

# # Clear existing data in the sheet
# sheet.clear()

# # Read the combined CSV file
# data_df = pd.read_csv(output_file)

# # Convert DataFrame to a list of lists, where each sublist is a row in the DataFrame
# data_list = data_df.values.tolist()

# # Insert column headers as the first row
# data_list.insert(0, data_df.columns.tolist())

# # Update the sheet with the new data
# sheet.update('A1', data_list)

# print('Google Sheet has been updated with new data.')

