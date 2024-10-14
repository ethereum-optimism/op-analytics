import os

from op_coreutils.env import env_get_or_none
from google.oauth2 import service_account


def get_credentials():
    if credentials := env_get_or_none("GOOGLE_SERVICE_ACCOUNT"):
        if isinstance(credentials, str) and os.path.isfile(credentials):
            return service_account.Credentials.from_service_account_file(credentials)
        else:
            return service_account.Credentials.from_service_account_info(credentials)
    return None
