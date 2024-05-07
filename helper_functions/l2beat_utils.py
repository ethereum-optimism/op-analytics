import requests as r
import pandas as pd
import re

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
        tvl_df = get_l2beat_activity_data('tvl',granularity)

        combined_df = tvl_df.merge(activity_df, on=['timestamp','chain'],how='outer')

        return combined_df



def get_l2beat_metadata():
        df = pd.DataFrame(columns=['layer', 'name', 'chainId', 'explorerUrl', 'category', 'slug'])

        # GitHub API URL for the specified repository and directory
        base_url = "https://api.github.com/repos/l2beat/l2beat/contents/packages/config/src"

        # Folders to navigate
        folders = ["chains", "layer2s", "layer3s"]

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
        # Function to check if any config item contains the word 'upcoming'
        def check_upcoming(configs):
                return any('upcoming' in config.lower() for config in configs)
        def determine_provider(file_content):
                if 'opStackL2' in file_content:
                        return 'OP Stack'
                return extract_data(file_content, patterns['provider'])
        # Navigate through the folders
        for folder in folders:
        # Request the content of the folder
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
                                
                                # Prepare data with extracted values or defaults where necessary
                                data = {
                                        'layer': folder[:-1],  # Dynamically set the layer based on folder name
                                        'name': extract_data(file_content, patterns['name']),
                                        'chainId': extract_data(file_content, patterns['chainId']),
                                        'explorerUrl': extract_data(file_content, patterns['explorerUrl']),
                                        'category': extract_data(file_content, patterns['category']) if folder in ['layer2s', 'layer3s'] else None,
                                        'slug': extract_data(file_content, patterns['slug']) or file['name'].replace('.ts', ''),  # Filename as fallback slug
                                        'provider': determine_provider(file_content),  # Determine provider with custom logic
                                        # 'configs': configs,  # Extract imports as a list
                                        'is_upcoming': is_upcoming
                                }
                                
                                # Add the data dictionary to our list
                                data_list.append(data)
                        else:
                                print(f"{base_url}/{folder}")
                                print(file + " Response is not in the expected format.")
                
                # Convert the list of dictionaries to a DataFrame and concatenate with the main DataFrame
                df = pd.concat([df, pd.DataFrame(data_list)], ignore_index=True)

        return df

        # Display the DataFrame