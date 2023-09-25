import pandas as pd
import json

def generate_table_from_tokenlist(data):
        # Create a list of dictionaries, where each dictionary represents a token
        tokens_list = []
        for token in data['tokens']:
                token_dict = {
                        'chainId': token['chainId'],
                        'address': token['address'],
                        'name': token['name'],
                        'symbol': token['symbol'],
                        'decimals': token['decimals'],
                        # **{f'extensions_{k}': v for k, v in token['extensions'].items()}  # Add the extensions as separate columns
                }
                # print(token_dict)
                tokens_list.append(token_dict)

        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(tokens_list)

        return df

def generate_op_table_from_tokenlist(data):
    # Create a list of dictionaries, where each dictionary represents a token
    tokens_list = []
    for token in data['tokens']:
        token_dict = {
            'chainId': token['chainId'],
            'address': token['address'],
            'name': token['name'],
            'symbol': token['symbol'],
            'decimals': token['decimals'],
            'optimismBridgeAddress': token['extensions'].get('optimismBridgeAddress', ''),
            # **{f'extensions_{k}': v for k, v in token['extensions'].items()}  # Add the extensions as separate columns
        }
        # print(token_dict)
        tokens_list.append(token_dict)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(tokens_list)

    return df
