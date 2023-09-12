import requests as r

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

def get_mb_query_response(url_base, session, card_id):
        url = f"{url_base}/api/card/{card_id}/query/json"

        headers = {
                "Content-Type": "application/json",
                "X-Metabase-Session": session
        }

        try:
                response = r.post(url, headers=headers)
                response.raise_for_status()  # Check if the request was successful
                return response.json()  # Parse and return the JSON response
        except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                return None