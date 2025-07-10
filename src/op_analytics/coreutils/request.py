import time
import re
from urllib3.util.retry import Retry

import requests
from requests.adapters import HTTPAdapter
from typing import Any
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


DEFAULT_RETRY_STRATEGY = Retry(
    total=5,  # Total number of retries
    backoff_factor=2,  # The backoff factor (2 seconds, then 4, 8...)
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)


def mask_api_key_in_text(text: str) -> str:
    """
    Mask API keys in any text (error messages, URLs, etc.).

    This function looks for common API key patterns and masks them for secure logging.

    Args:
        text: Text that may contain API keys

    Returns:
        Text with API keys masked
    """
    # Pattern to find DeFiLlama API keys
    defillama_pattern = r"(https://pro-api\.llama\.fi/)([^/\s]+)"

    def replace_defillama_match(match):
        base_url, api_key = match.groups()
        if len(api_key) > 8:
            masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:]
        else:
            masked_key = "*" * len(api_key)
        return f"{base_url}{masked_key}"

    # Apply DeFiLlama masking
    text = re.sub(defillama_pattern, replace_defillama_match, text)

    # Add more patterns here for other APIs as needed
    # Example for generic API keys in URLs:
    # generic_pattern = r"(https?://[^/]+/)([a-zA-Z0-9]{20,})"
    # text = re.sub(generic_pattern, replace_generic_match, text)

    return text


def new_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=DEFAULT_RETRY_STRATEGY)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def get_data(
    session: requests.Session,
    url: str,
    headers: dict[str, str] | None = None,
    retry_attempts: int | None = None,
    params: dict[str, Any] | None = None,
    emit_log: bool = True,
    timeout: int | None = None,  # this is the timeout for a single call
    retries_timeout: int = 600,  # this is the total timeout for all retries
    retries_wait_initial: int = 10,
    retries_wait_max: int = 60,
):
    """Helper function to reuse an existing HTTP session to fetch data from a URL.

    - Reports timing.
    - Raises for HTTP error status codes (>400) and checks for 200 status code.
    """

    headers = headers or {"Content-Type": "application/json"}

    # Do not retry on invalid json responses.
    if retry_attempts is None:
        return _get_data(
            session=session,
            url=url,
            headers=headers,
            params=params,
            emit_log=emit_log,
            timeout=timeout,
        )

    # Custom retry logic to avoid stamina's API key exposure
    last_exception = None
    wait_time = retries_wait_initial
    total_wait_time = 0

    for attempt in range(1, retry_attempts + 1):
        try:
            if attempt > 1:
                # Mask API keys in the URL before logging retry attempts
                masked_url = mask_api_key_in_text(url)
                log.warning(f"retry attempt {masked_url}", attempt=attempt)

            return _get_data(
                session=session,
                url=url,
                headers=headers,
                params=params,
                emit_log=emit_log,
                timeout=timeout,
            )

        except Exception as e:
            last_exception = e

            # Mask API keys in the exception message before logging
            masked_error = mask_api_key_in_text(str(e))
            log.error(f"retrying exception {masked_error}")

            # If this is the last attempt, don't wait
            if attempt == retry_attempts:
                break

            # Check if we've exceeded the total timeout
            if total_wait_time >= retries_timeout:
                log.error(
                    "retry timeout exceeded",
                    total_wait_time=total_wait_time,
                    retries_timeout=retries_timeout,
                )
                break

            # Wait before the next attempt
            actual_wait = min(wait_time, retries_wait_max)
            if total_wait_time + actual_wait > retries_timeout:
                actual_wait = retries_timeout - total_wait_time

            if actual_wait > 0:
                log.warning(
                    f"waiting {actual_wait}s before retry", attempt=attempt, wait_time=actual_wait
                )
                time.sleep(actual_wait)
                total_wait_time += actual_wait

            # Exponential backoff
            wait_time *= 2

    # If we get here, all attempts failed
    if last_exception:
        # Mask API keys in the final exception message
        masked_error = mask_api_key_in_text(str(last_exception))
        raise type(last_exception)(masked_error) from last_exception
    else:
        raise Exception("All retry attempts failed")


def _get_data(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    emit_log: bool = True,
    timeout: int | None = None,
):
    start = time.time()
    resp = session.request(method="GET", url=url, headers=headers, params=params, timeout=timeout)

    resp.raise_for_status()

    if resp.status_code != 200:
        # Mask API keys in the URL before including in exception
        masked_url = mask_api_key_in_text(url)
        raise Exception(f"status={resp.status_code}, url={masked_url!r}")

    if emit_log:
        # Mask API keys in the URL before logging
        masked_url = mask_api_key_in_text(url)
        log.info(f"Fetched from {masked_url}: {time.time() - start:.2f} seconds")
    return resp.json()
