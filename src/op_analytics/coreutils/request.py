import time
from urllib3.util.retry import Retry

import requests
import stamina
from requests.adapters import HTTPAdapter
from typing import Any
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


DEFAULT_RETRY_STRATEGY = Retry(
    total=5,  # Total number of retries
    backoff_factor=2,  # The backoff factor (2 seconds, then 4, 8...)
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)


def new_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=DEFAULT_RETRY_STRATEGY)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def retry_logger(exc: Exception) -> bool:
    log.error(f"retrying exception {exc}")
    return True


def get_data(
    session: requests.Session,
    url: str,
    headers: dict[str, str] | None = None,
    retry_attempts: int | None = None,
    params: dict[str, Any] | None = None,
):
    """Helper function to reuse an existing HTTP session to fetch data from a URL.

    - Reports timing.
    - Raises for HTTP error status codes (>400) and checks for 200 status code.
    """

    headers = headers or {"Content-Type": "application/json"}

    # Do not retry on invalid json responses.
    if retry_attempts is None:
        return _get_data(session, url, headers, params)

    # Retry on exceptions.
    for attempt in stamina.retry_context(
        on=retry_logger,
        attempts=retry_attempts,
        timeout=600,
        wait_initial=10,
        wait_max=60,
    ):
        with attempt:
            if attempt.num > 1:
                log.warning(f"retry attempt {url}", attempt=attempt.num)
            return _get_data(session, url, headers, params)


def _get_data(session: requests.Session, url: str, headers: dict[str, str], params: dict[str, Any]):
    start = time.time()
    resp = session.request(method="GET", url=url, headers=headers, params=params)

    resp.raise_for_status()

    if resp.status_code != 200:
        raise Exception(f"status={resp.status_code}, url={url!r}")

    log.info(f"Fetched from {url}: {time.time() - start:.2f} seconds")
    return resp.json()
