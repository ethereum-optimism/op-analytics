import requests as r
import time

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
                        print(response.raise_for_status())
                        response_content = response.json()
                        print(str(response_content)[:100])
                        
                        return response.json()  # Parse and return the JSON response
                except r.exceptions.RequestException as e:
                        print(f"An error occurred: {e}")
                if retry < num_retries - 1:
                        print(f"Retrying in 1 second (Retry {retry + 1}/{num_retries})...")
                        time.sleep(1)
                else:
                        print(f"Maximum number of retries ({num_retries}) reached. Giving up.")
                        return None