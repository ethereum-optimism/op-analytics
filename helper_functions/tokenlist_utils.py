import pandas as pd
import json

def extract_from_extensions(extensions_json, keys):
    try:
        extensions = json.loads(extensions_json)
        return [extensions.get(key) for key in keys]
    except json.JSONDecodeError:
        return [None] * len(keys)
    
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
    for index, token in enumerate(data['tokens']):
        try:
            token_dict = {
                'chainId': token.get('chainId'),
                'address': token.get('address'),
                'name': token.get('name'),
                'symbol': token.get('symbol'),
                'decimals': token.get('decimals'),
                'logoURI': token.get('logoURI'),
                'extensions': json.dumps(token.get('extensions', {}))
            }
            tokens_list.append(token_dict)
        except Exception as e:
            print(f"Error processing token at index {index}: {e}")
            print(f"Token data: {token}")

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(tokens_list)
    # Extract 'opListId' and 'opTokenId' from extensions
    df[['opListId', 'opTokenId']] = df['extensions'].apply(lambda x: extract_from_extensions(x, ['opListId', 'opTokenId'])).tolist()

    return df
