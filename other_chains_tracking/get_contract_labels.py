#!/usr/bin/env python
# coding: utf-8

# In[ ]:


print('start contract labels')
import sys
sys.path.append("../helper_functions")
import google_bq_utils as bqu
import clickhouse_utils as ch
sys.path.pop()

import numpy as np
import pandas as pd
import time
import requests as r


# In[ ]:


api_configs = {
    'old_gtp': {
        'url': 'https://api.growthepie.xyz/v1/contracts.json',
        'table_name': 'contract_labels_growthepie',
        'unique_keys' :['address','origin_key']
    },
    'oli_gtp': {
        'url': 'https://api.growthepie.xyz/test/oli_labels.json',
        'table_name': 'oli_contract_labels_growthepie',
        'unique_keys' :['address','chain_id']
    },
    'oss': {
        'url': 'https://api.growthepie.xyz/test/oss_projects.json',
        'table_name': 'oss_project_labels_growthepie',
        'unique_keys' :['id']
    }
}

parquet_configs = {
    'gtp_projects': {
        'url': 'https://api.growthepie.xyz/v1/labels/projects.parquet',
        'table_name': 'labels_projects_growthepie',
        'unique_keys' :['owner_project']
    },
    'gtp_50k_parquet': {
        'url': 'https://api.growthepie.xyz/v1/labels/export_labels_top50k.parquet',
        'table_name': 'labels_top50k_growthepie',
        'unique_keys' :['address','chain_id']
    },
}


# In[ ]:


def fetch_and_process_data(url):
    response = r.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    if 'address' in df.columns:
        df['address_original'] = df['address']
        df['address'] = df['address'].str.lower()

    # If 'is_factory_contract' exists, fill nulls with 'false'
    if 'is_factory_contract' in df.columns:
        df['is_factory_contract'] = df['is_factory_contract'].fillna('false')

    # Convert all object columns to strings
    object_columns = df.select_dtypes(include=['object']).columns
    df[object_columns] = df[object_columns].astype(str)
    return df

# Iterate through the configurations
for config_name, config in api_configs.items():
    config_table_name = config['table_name']
    config_unique_keys = config['unique_keys']

    df = fetch_and_process_data(config['url'])
    # You can now use df and config['table_name'] as needed
    print(f"Processed data for {config_name}:")
    print(f"Table name: {config_table_name}")
    # ch.write_df_to_clickhouse(df, config['table_name'], if_exists='replace')
    ch.append_and_upsert_df_to_clickhouse(df, table_name = config_table_name,unique_keys=config_unique_keys)
    print('clickhouse complete')
    
    # bqu.write_df_to_bq_table(df, config['table_name'])
    bqu.append_and_upsert_df_to_bq_table(df, table_id = config_table_name,unique_keys=config_unique_keys)
    print('bq complete')
    print(f"Table name: {config_table_name}")
    print(f"Number of rows: {len(df)}")
    print("---")


# In[ ]:


for config_name, config in parquet_configs.items():
        config_url = config['url']
        config_table_name = config['table_name']
        config_unique_keys = config['unique_keys']
        print(f'{config_table_name} - {config_unique_keys}')
        df = pd.read_parquet(config_url)
        if 'chain_id' in df.columns:
                df['og_chain_id'] = df['chain_id']
                df['chain_id'] = df['chain_id'].astype(str).str.replace('eip155-','')
        if 'address' in df.columns:
                df['og_address'] = df['address']
                df['address'] = df['address'].astype(str).str.lower()
        if 'deployment_date' in df.columns:
                # Convert to datetime, keeping NaT values
                df['deployment_date'] = pd.to_datetime(df['deployment_date'], errors='coerce')
                # Convert datetime to string in ISO format
                df['deployment_date'] = df['deployment_date'].dt.strftime('%Y-%m-%d')
                # Replace NaT with None
                df['deployment_date'] = df['deployment_date'].where(df['deployment_date'].notna(), None)

        # print(df[df['owner_project'].notnull()].sample(5))

        # print(df.dtypes)
        ch.append_and_upsert_df_to_clickhouse(df, config_table_name, unique_keys=config_unique_keys)
        bqu.append_and_upsert_df_to_bq_table(df, config_table_name, unique_keys=config_unique_keys)
        # ch.write_df_to_clickhouse(df, config_table_name, if_exists='replace')

