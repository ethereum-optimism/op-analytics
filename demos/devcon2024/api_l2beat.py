import pandas as pd
import requests as r
import time
import api_utilities as ap

# TODO Make Concurrent

url_base = "https://l2beat.com/api/"

default_query_range = "1y" # range can be any of ['7d', '30d', '90d', '180d', '1y', 'max'], defaults to 30d
max_retries = 5



def get_l2beat_summary():
    session = ap.new_session()

    summary_url = f"{url_base}scaling/summary"
    print(summary_url)

    summary = ap.get_data(session, summary_url)

    projects_summary = list(summary["data"]["projects"].values())
    df = pd.DataFrame(projects_summary)
    return df

def process_response(l2beat_slug, response_json, metric_value):
    if response_json.get('success') == True: #if success doesn't exist, then make it false
        chart_data = response_json['data']['chart']
        column_types = chart_data['types']
        data_values = chart_data['data']

        # Create DataFrame
        df = pd.DataFrame(data_values, columns=column_types)
        df['slug'] = l2beat_slug

        # Convert timestamp to datetime if it exists
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        if 'count' in df.columns:
            df.rename(columns={'count':metric_value}, inplace=True)

        return df
    
    elif response_json.get('success') != True:
        error = response_json.get('error')
        # print(f"Slug: {l2beat_slug}, Error: {error}")
        return None

    else:
        print(f"Slug: {l2beat_slug}, The response was not successful")
        return None

def get_single_project(l2beat_slug, url_type, query_range = default_query_range):
        session = ap.new_session()
        
        project_url = f"{url_base}scaling/{url_type}/{l2beat_slug}?range={query_range}"
        # print(project_url)

        if url_type == 'activity':
            metric = 'transactions_per_day'
        elif url_type == 'tvl':
            metric = 'assets_onchain_usd'
        else:
            metric = 'unknown'

        try:
            response = ap.get_data(session, project_url)
            response_df = process_response(l2beat_slug, response, metric)
            return response_df
        
        except r.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            time.sleep(1)
            return None


def get_all_projects_data(summary_df, url_type = 'activity', query_range=default_query_range):
    project_list = summary_df['slug']
    num_projects = len(project_list)
    print(f"{url_type} API, Chains to run: {num_projects}")

    data_dfs = []
    i = 0
    for slug in project_list:
        data = get_single_project(slug, url_type, query_range)
        i += 1
        if data is not None:
            # Convert the dictionary to a DataFrame
            df = pd.DataFrame(data)
            # Add a column for the project slug if it's not already in the data
            if 'slug' not in df.columns:
                df['slug'] = slug
            data_dfs.append(df)
        
        if i % 25 == 0:
            print(f"{i} / {num_projects} completed")
    
    if not data_dfs:
        print("No valid data retrieved for any project.")
        return None
    
    print("All projects completed")

    final_dfs = pd.concat(data_dfs, ignore_index=True)
    return final_dfs
