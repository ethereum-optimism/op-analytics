import urllib3
from urllib3.util.retry import Retry

from op_coreutils.logger import structlog

log = structlog.get_logger()


DEFAULT_RETRY_STRATEGY = Retry(
    total=5,  # Total number of retries
    backoff_factor=1,  # The backoff factor (1 second, then 2, 4, 8...)
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)


def new_session():
    http = urllib3.PoolManager(retries=DEFAULT_RETRY_STRATEGY)

    return http
