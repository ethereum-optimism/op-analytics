import requests as r
import pandas as pd
import re
from datetime import datetime, timezone

api_string = 'https://api.l2beat.com/api/'
# https://api.l2beat.com/api/tvl
# https://api.l2beat.com/api/activity
# https://l2beat.com/api/tvl/scaling.json
# https://l2beat.com/api/tvl/optimism.json

def get_l2beat_activity_data(data='activity',granularity='daily'):
        df = pd.DataFrame()

        api_url = api_string + data
        response = r.get(api_url)
        response.raise_for_status()  # Check if the request was successful
        json_data = response.json()['projects']
        
        # Create an empty list to collect rows
        rows_list = []
        
        # Iterate over the chains
        for chain_name in json_data:
        # if chain_name != 'combined':
                if data == 'activity':
                        daily_data = json_data[chain_name][granularity]['data']
                        types = json_data[chain_name][granularity]['types']
                elif data == 'tvl':
                        daily_data = json_data[chain_name]['charts'][granularity]['data']
                        types = json_data[chain_name]['charts'][granularity]['types']
                else:
                        print('not configured - need to configure for this API endpoint')
                        return

                # Iterate through each day's data
                for day_data in daily_data:
                        # Create a dictionary for each day's data
                        data_dict = dict(zip(types, day_data))
                        data_dict['chain'] = chain_name  # Add the chain name to the dictionary
                        rows_list.append(data_dict)
        
        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows_list)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') 
        return df

def get_all_l2beat_data(granularity='daily'):
        activity_df = get_l2beat_activity_data('activity',granularity)
        aop_df = get_l2beat_activity_data('tvl',granularity)

        combined_df = aop_df.merge(activity_df, on=['timestamp','chain'],how='outer')

        return combined_df

def get_daily_aoc_by_token():
        api_url = 'https://api.l2beat.com/api/tvl'
        response = r.get(api_url)
        response.raise_for_status()
        data = response.json()

        rows = []
        # timestamp = datetime.fromtimestamp(data['timestamp'] / 1000).strftime('%Y-%m-%d')
        # Use today's date in UTC
        timestamp = datetime.now(timezone.utc)
        timestamp_date = timestamp.strftime('%Y-%m-%d')

        for project_name, project_data in data['projects'].items():
                for token_type, tokens in project_data['tokens'].items():
                        for token in tokens:
                                rows.append({
                                'dt': timestamp_date,
                                'project': project_name,
                                'token_type': token_type,
                                'asset_id': token['assetId'],
                                'address': token['address'],
                                'source_chain': token['chain'],
                                'source_chain_id': token['chainId'],
                                'source': token['source'],
                                'usd_value': token['usdValue'],
                                'dt_updated': timestamp
                                })
        df = pd.DataFrame(rows)
        df["dt"] = pd.to_datetime(df["dt"], errors='coerce')
        return df


def get_l2beat_metadata():
        df = pd.DataFrame(columns=['layer', 'name', 'chainId', 'explorerUrl', 'category', 'slug','isArchived'])

        # GitHub API URL for the specified repository and directory
        base_url = "https://api.github.com/repos/l2beat/l2beat/contents/packages/config/src"

        # Folders to navigate
        folders = ["projects/layer2s", "projects/layer3s", "chains"] # "chains"

        # Regular expression patterns for parsing TypeScript files
        # Regular expression patterns for parsing TypeScript files
        patterns = {
                'name': r"name: '([^']+)'",
                'chainId': r"chainId: (\d+)",
                'explorerUrl': r"explorerUrl: '([^']+)'",
                # Improved patterns to match multiline and nested structures
                'category': r"display:.*?category: '([^']+)'",
                'slug': r"slug: '([^']+)'",
                'imports': r"import {([^}]+)} from",
                'provider': r"display:.*?provider: '([^']+)'",  # Updated to handle multiline and nested content
                'hostChain': r"hostChain: ProjectId\('(\w+)'\)",
                'websites': r"websites: \[([^\]]+)\]",
                'documentation': r"documentation: \[([^\]]+)\]",
                'repositories': r"repositories: \[([^\]]+)\]",
                'rpcUrl': r"rpcUrl: '([^']+)'",
                'project_discovery': r"const discovery = new ProjectDiscovery\('([^']+)'\)",
                'isArchived': r"isArchived: (true|false)"
        }
        
        # Function to extract data using regular expressions
        def extract_data(text, pattern):
                match = re.search(pattern, text, re.DOTALL)  # re.DOTALL allows '.' to match newlines
                return match.group(1).strip() if match else None
        
        # Function to safely get file content, returns None if URL is invalid
        def safe_get_content(url):
                if url:
                        try:
                                return r.get(url).text
                        except r.exceptions.MissingSchema:
                                print(f"Invalid URL: {url}")
                return None
        def extract_imports(text):
                matches = re.findall(patterns['imports'], text, re.DOTALL)
                imports = set()
                for match in matches:
                        # Split multiple imports in one line and strip whitespace
                        items = match.split(',')
                        items = [item.strip() for item in items if item.strip()]
                        imports.update(items)
                return list(imports)
        # Function to check if any config item contains the word 'UpcomingL'
        def check_upcoming(configs):
                return any('upcomingl' in config.lower() for config in configs)
        def determine_provider(file_content):
                if 'opStackL' in file_content:  # opStackL2 or opStackL3
                        return 'OP Stack'
                elif 'polygonCDKStack' in file_content:
                        return 'Polygon CDK'
                elif 'orbitStackL' in file_content:  # orbitStackL2 or orbitStackL3
                        return 'Arbitrum Orbit'
                elif ("'zkSync'" in file_content) or ("'ZK Stack'" in file_content):
                        return 'ZK Stack'
                else:
                        return extract_data(file_content, patterns['provider'])
        def determine_layer(folder_name):
                if 'chain' in folder_name:
                        return 'L1'
                elif 'layer2' in folder_name:
                        return 'L2'
                elif 'layer3' in folder_name:
                        return 'L3'
                else:
                        return folder_name
        # Navigate through the folders
        for folder in folders:
        # Request the content of the folder
                folder_name = folder.split("/")[-1]
                response = r.get(f"{base_url}/{folder}").json()
                
                # Initialize a list to collect data dictionaries before appending to DataFrame
                data_list = []
                
                # Iterate through each file in the folder
                for file in response:
                        if isinstance(response, list):
                                if file['name'] in ['index.ts', 'index.test.ts'] or 'download_url' not in file or file['download_url'] is None:
                                        continue  # Skip these files
                                
                                # Get the content of the file safely
                                file_content = safe_get_content(file['download_url'])
                                if file_content is None:  # Skip if content couldn't be retrieved
                                        continue

                                # Extract imports and check for upcoming keyword
                                configs = extract_imports(file_content)
                                is_upcoming = check_upcoming(configs)
                                
                                layer_name = determine_layer(folder_name)
                                # Prepare data with extracted values or defaults where necessary
                                slug = extract_data(file_content, patterns['project_discovery'])
                                if not slug:  # If project_discovery is not found, use slug pattern
                                        slug = extract_data(file_content, patterns['slug'])
                                
                                is_archived = extract_data(file_content, patterns['isArchived'])
                                is_archived = True if is_archived == 'true' else False

                                # Combine to find what chains we care about for charts
                                is_current_chain = not is_upcoming and not is_archived

                                # Prepare data with extracted values or defaults where necessary
                                data = {
                                        'layer': layer_name,  # Dynamically set the layer based on folder name
                                        'slug': slug or file['name'].replace('.ts', ''),  # Filename as fallback slug
                                        'file_name': file['name'].replace('.ts', ''),  # Filename as fallback slug
                                        'chainId': extract_data(file_content, patterns['chainId']),
                                        'name': extract_data(file_content, patterns['name']),
                                        'explorerUrl': extract_data(file_content, patterns['explorerUrl']),
                                        'rpcUrl': extract_data(file_content, patterns['rpcUrl']),
                                        'category': extract_data(file_content, patterns['category']) if layer_name in ['L2', 'L3'] else None,
                                        'provider': determine_provider(file_content),  # Determine provider with custom logic
                                        'hostChain': extract_data(file_content, patterns['hostChain']),
                                        'is_upcoming': is_upcoming,
                                        'is_archived': is_archived,
                                        'is_current_chain': is_current_chain,
                                        'websites': extract_data(file_content, patterns['websites']),
                                        'documentation': extract_data(file_content, patterns['documentation']),
                                        'repositories': extract_data(file_content, patterns['repositories'])
                                        # 'configs': configs,  # Extract imports as a list
                                }
                                
                                # Add the data dictionary to our list
                                data_list.append(data)
                        else:
                                print(f"{base_url}/{folder}")
                                print(file + " Response is not in the expected format.")
                
                # Convert the list of dictionaries to a DataFrame and concatenate with the main DataFrame
                df = pd.concat([df, pd.DataFrame(data_list)], ignore_index=True)

                # Map sub-providers to the top-level entity
                df['provider_entity'] = df['provider']  # Initialize with provider values
                
                df.loc[df['provider'].str.contains('Arbitrum', case=False, na=False), 'provider_entity'] = 'Arbitrum: Orbit'
                df.loc[df['provider'].isin(['OP Stack', 'OVM']), 'provider_entity'] = 'Optimism: OP Stack'
                df.loc[df['provider'].str.contains('Polygon', case=False, na=False), 'provider_entity'] = 'Polygon: CDK'
                df.loc[df['provider'].str.contains('zkSync', case=False, na=False) | df['provider'].str.contains('ZK Stack', case=False, na=False), 'provider_entity'] = 'zkSync: ZK Stack'
                df.loc[df['provider'].isin(['Starkware', 'Starknet','StarkEx']), 'provider_entity'] = 'Starkware: Starknet Stack'

        return df

        # Display the DataFrame