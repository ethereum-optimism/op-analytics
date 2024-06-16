#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# github: https://github.com/ethereum-lists/chains
api_url = 'https://chainid.network/chains_mini.json'


# In[ ]:


import pandas as pd
import requests as r
import sys

sys.path.append("../helper_functions")
import pandas_utils as pu
import google_bq_utils as bqu
sys.path.pop()


# In[ ]:


# Send a GET request to the API
response = r.get(api_url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    data = response.json()

    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Print the DataFrame
    # print(df.sample(5))
else:
    print(f"Error: {response.status_code}")


# In[ ]:


df = pu.flatten_nested_data(df, 'nativeCurrency')


# In[ ]:


def remove_words(name, words_to_remove):
    """
    Remove specified words from the name string and trim remaining spaces,
    while preserving the original capitalization.
    """
    # Split the name into words
    name_words = name.split()
    
    # Remove the specified words from the list of words
    cleaned_words = [word for word in name_words if word.lower() not in [w.lower() for w in words_to_remove]]
    
    # Join the remaining words back into a string
    cleaned_name = ' '.join(cleaned_words)
    
    return cleaned_name


# In[ ]:


# Generate Clean Name
words_to_remove = [
        'Mainnet','Network'
]

df['clean_chain_name'] = df.apply(lambda row: remove_words(row['name'], words_to_remove), axis=1)

# Convert columns to strings
df['rpc'] = df['rpc'].apply(lambda x: str(x))
df['faucets'] = df['faucets'].apply(lambda x: str(x))
df['infoURL'] = df['infoURL'].apply(lambda x: str(x))


# In[ ]:


# df['name'] = df['name'].astype(str)
# df.dtypes
# df[df['name'].isnull()]


# In[ ]:


# df.sample(10)


# In[ ]:


bqu.write_df_to_bq_table(df, 'ethereum_lists_chains')

