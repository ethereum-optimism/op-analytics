import json
import os

from google.oauth2 import service_account


def get_credentials():
    if credentials := os.getenv("GOOGLE_AUTH"):
        return service_account.Credentials.from_service_account_info(json.loads(credentials))
    return None
