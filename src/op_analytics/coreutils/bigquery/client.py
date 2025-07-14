from unittest.mock import MagicMock

from google.cloud import bigquery

from op_analytics.coreutils.env.aware import OPLabsEnvironment, current_environment
from op_analytics.coreutils.gcpauth import get_credentials
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

_CLIENT: bigquery.Client | MagicMock | None = None


# This dataset is used to store staging tables used in upsert operations.
# It is already configured with a default table expiration time of 1 day.
UPSERTS_TEMP_DATASET = "temp_upserts"


def init_client():
    """BigQuery client with environment and testing awareness."""
    global _CLIENT

    if _CLIENT is None:
        current_env = current_environment()

        if current_env == OPLabsEnvironment.PROD:
            _CLIENT = bigquery.Client(credentials=get_credentials())
        else:
            log.warning("Using MagicMock for BigQuery client in non-PROD environment.")
            log.warning("Set OPLABS_ENV=PROD to use the real BigQuery client.")

            # MagicMock is used when running tests or when the PROD env is not specified.
            # This is helpful to collect data and iterate on transformations without
            # accidentally writing data to BigQuery before the final logic is in place.
            _CLIENT = MagicMock()

    if _CLIENT is None:
        raise RuntimeError("BigQuery client was not properly initialized.")

    return _CLIENT
