import os

from op_analytics.coreutils.env import env_get_or_none
from op_analytics.coreutils.logger import structlog
from google.oauth2 import service_account

log = structlog.get_logger()


def get_credentials():
    if credentials := env_get_or_none("GOOGLE_SERVICE_ACCOUNT"):
        msg = "Found vault variable GOOGLE_SERVICE_ACCOUNT"

        if isinstance(credentials, str) and os.path.isfile(credentials):
            log.info(f"{msg} (points to file)")
            return service_account.Credentials.from_service_account_file(credentials)
        else:
            log.info(f"{msg} (has JSON key)")
            return service_account.Credentials.from_service_account_info(credentials)

    log.info(
        "gcpauth.py: vault variable GOOGLE_SERVICE_ACCOUNT is not configured. Will use default auth process."
    )
    return None
