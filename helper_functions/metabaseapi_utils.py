import requests as r
import time
import json

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
            print(response_content_str[:100])

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
                        print(f"'Failed' status detected. Retrying in 60 seconds (Retry {retry + 1}/{num_retries})...")
                        time.sleep(60)
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
                print(f"Retrying in 1 second due to exception (Retry {retry + 1}/{num_retries})...")
                time.sleep(1)
            else:
                print(f"Maximum number of retries ({num_retries}) reached due to exception. Giving up.")
                return None