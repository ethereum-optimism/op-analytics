import time
from urllib3.util.retry import Retry

import requests
from requests.adapters import HTTPAdapter

from op_coreutils.logger import structlog

log = structlog.get_logger()


DEFAULT_RETRY_STRATEGY = Retry(
    total=5,  # Total number of retries
    backoff_factor=1,  # The backoff factor (1 second, then 2, 4, 8...)
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)


def new_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=DEFAULT_RETRY_STRATEGY)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def get_data(session: requests.Session, url: str, headers: dict[str, str] | None = None):
    """Helper function to reuse an existing HTTP session to fetch data from a URL.

    - Reports timing.
    - Raises for HTTP error status codes (>400) and checks for 200 status code.
    """
    start = time.time()

    headers = headers or {"Content-Type": "application/json"}

    resp = session.request(method="GET", url=url, headers=headers)

    resp.raise_for_status()

    if resp.status_code != 200:
        raise Exception(f"status={resp.status}, url={url!r}")

    log.info(f"Fetched from {url}: {time.time() - start:.2f} seconds")
    return resp.json()
