import requests as r
import time
import json
import os
import pandas as pd
import dotenv
import duneapi_utils as d
dotenv.load_dotenv()

def get_mb_session_key(url_base, name, pw):
        url = url_base + "/api/session"
        payload = {
        "username": name,
        "password": pw
        }

        headers = {
        "Content-Type": "application/json"
        }

        response = r.post(url, json=payload, headers=headers)
        return response.json()['id']

def get_mb_query_response(url_base, session, card_id, num_retries=3):
    url = f"{url_base}/api/card/{card_id}/query/json"

    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": session
    }

    for retry in range(num_retries):
        try:
            response = r.post(url, headers=headers)
            print(response)
            response.raise_for_status()  # Check if the request was successful
            response_content = response.json()
            response_content_str = json.dumps(response_content)
            print(response_content_str[:250])

            # Check the type of response_content
            if isinstance(response_content, list) and len(response_content) > 0:
                # It's a list with at least one element
                print(response_content[0])
                # Rest of your code to handle the response
                return response_content  # If it's a list, return the JSON response

            elif isinstance(response_content, dict) and 'status' in response_content:
                # It's a dictionary with a 'status' key
                if response_content['status'] == 'failed':
                    # Handle 'failed' status
                    if retry < num_retries - 1:
                        print(f"'Failed' status detected. Retrying in 10 seconds (Retry {retry + 1}/{num_retries})...")
                        time.sleep(10)
                        continue
                    else:
                        print(f"Maximum number of retries ({num_retries}) reached with 'failed' status. Giving up.")
                        return None

            else:
                print("Unexpected response content format:", response_content)
                # Handle unexpected format here

        except r.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            if retry < num_retries - 1:
                print(f"Retrying in 10 seconds due to exception (Retry {retry + 1}/{num_retries})...")
                time.sleep(10)
            else:
                print(f"Maximum number of retries ({num_retries}) reached due to exception. Giving up.")
                return None

def get_session_id(mb_url_base, mb_name, mb_pw):
    try: #do you already have a session
        session_id = os.environ["MS_METABASE_SESSION_ID"]
        # Test if session ID works
        resp = get_mb_query_response(mb_url_base, session_id, 42, num_retries = 1)
        if resp is None:
            raise ValueError("Response is None")
    except: #if not, make one
        print('creating new session')
        session_id = get_mb_session_key(mb_url_base,mb_name,mb_pw)
    # print(session_id)
    if (os.environ["IS_RUNNING_LOCAL"]):
            print(session_id)
    return session_id

def query_response_to_dune(session_id, mb_url_base, query_num, dune_table_name, dune_table_description):
    # Map Chain Names
    chain_mappings = {
        'zora': 'Zora',
        'pgn': 'Public Goods Network',
        'base': 'Base Mainnet'
        # Add more mappings as needed
    }
    print('query number: ' + str(query_num))

    resp = get_mb_query_response(mb_url_base, session_id, query_num, num_retries = 3)

    try:
        data_df = pd.DataFrame(resp)
    except ValueError as e:
        print(f"Error in creating DataFrame: {e}")

    print("Type of response:", type(resp))

    if resp:
        print("First element of the list:", resp[0])
    else:
        print("The list is empty")

    keys_set = {frozenset(d.keys()) for d in resp if isinstance(d, dict)}
    if len(keys_set) > 1:
        print("Dictionaries have different sets of keys.")
    else:
        print("All dictionaries have the same set of keys.")

    standard_keys = set(resp[0].keys())

    for i, dic in enumerate(resp):
        # Get the set of keys of the current dictionary
        current_keys = set(dic.keys())
        
        # Check if the current set of keys matches the standard set of keys
        if current_keys != standard_keys:
            print(f"Dictionary at index {i} does not have the standard set of keys.")
            print(f"Dictionary keys: {current_keys}")
            print(f"Standard keys:   {standard_keys}")
            print(f"Dictionary content: {dic}")

    data_df['chain'] = data_df['chain'].replace(chain_mappings)

    print(data_df.columns)

    print(data_df.sample(5))
    # Post to Dune API
    d.write_dune_api_from_pandas(data_df, dune_table_name,dune_table_description)