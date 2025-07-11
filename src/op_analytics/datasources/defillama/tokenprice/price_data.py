from dataclasses import dataclass
from datetime import datetime, timedelta
import re

import polars as pl
import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import dt_fromepoch

log = structlog.get_logger()

# DeFiLlama Pro API endpoints
COINS_CHART_ENDPOINT = "https://pro-api.llama.fi/{api_key}/coins/chart/{coins}"
COINS_PRICES_CURRENT_ENDPOINT = "https://pro-api.llama.fi/{api_key}/coins/prices/current/{coins}"
COINS_PRICES_HISTORICAL_ENDPOINT = (
    "https://pro-api.llama.fi/{api_key}/coins/prices/historical/{timestamp}/{coins}"
)
COINS_BATCH_HISTORICAL_ENDPOINT = "https://pro-api.llama.fi/{api_key}/coins/batchHistorical"


TOKEN_PRICES_SCHEMA = pl.Schema(
    [
        ("token_id", pl.String),
        ("dt", pl.String),
        ("price_usd", pl.Float64),
        ("last_updated", pl.String),
    ]
)


def format_token_id_for_defillama(token_id: str) -> str:
    """
    Format token ID for DeFiLlama API.

    DeFiLlama requires CoinGecko slugs to be prefixed with 'coingecko:'.
    Chain:address format tokens are used as-is.

    Args:
        token_id: Token ID (either CoinGecko slug or chain:address format)

    Returns:
        Properly formatted token ID for DeFiLlama
    """
    # If it already has a chain prefix (contains ":"), use as-is
    if ":" in token_id:
        return token_id

    # Otherwise, it's a CoinGecko slug and needs the coingecko: prefix
    return f"coingecko:{token_id}"


def mask_api_key_in_url(url: str) -> str:
    """
    Mask the API key in a URL for secure logging.

    Args:
        url: URL that may contain an API key

    Returns:
        URL with API key masked
    """
    # Pattern to match the API key in DeFiLlama URLs
    pattern = r"(https://pro-api\.llama\.fi/)([^/]+)(/.*)"
    match = re.match(pattern, url)

    if match:
        base_url, api_key, path = match.groups()
        # Mask the API key, showing only first 4 and last 4 characters
        if len(api_key) > 8:
            masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:]
        else:
            masked_key = "*" * len(api_key)
        return f"{base_url}{masked_key}{path}"

    return url


def mask_api_key_in_text(text: str) -> str:
    """
    Mask API keys in any text (error messages, URLs, etc.).

    Args:
        text: Text that may contain API keys

    Returns:
        Text with API keys masked
    """
    # Pattern to find DeFiLlama API keys in any context
    pattern = r"(https://pro-api\.llama\.fi/)([^/\s]+)"

    def replace_match(match):
        base_url, api_key = match.groups()
        if len(api_key) > 8:
            masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:]
        else:
            masked_key = "*" * len(api_key)
        return f"{base_url}{masked_key}"

    return re.sub(pattern, replace_match, text)


def safe_get_data(session, url, **kwargs):
    """
    Wrapper around get_data that masks API keys in error messages.

    Args:
        session: HTTP session
        url: URL to fetch
        **kwargs: Additional arguments to pass to get_data

    Returns:
        Response data

    Raises:
        Exception with masked URL
    """
    try:
        return get_data(session, url, **kwargs)
    except Exception as e:
        # Mask the API key in the exception message
        error_message = mask_api_key_in_text(str(e))

        # Create a new exception with the masked message
        new_exception = type(e)(error_message)
        raise new_exception from e


@dataclass
class DefiLlamaTokenPrices:
    """Token price data from DeFiLlama Pro API."""

    df: pl.DataFrame

    @classmethod
    def fetch_prices_current(
        cls,
        token_ids: list[str],
        session: requests.Session | None = None,
    ) -> "DefiLlamaTokenPrices":
        """Fetch current prices for tokens.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            session: Optional requests session

        Returns:
            DefiLlamaTokenPrices instance with current price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # Format token IDs for DeFiLlama
        formatted_tokens = [format_token_id_for_defillama(tid) for tid in token_ids]
        tokens_str = ",".join(formatted_tokens)

        url = COINS_PRICES_CURRENT_ENDPOINT.format(api_key=api_key, coins=tokens_str)

        try:
            response = safe_get_data(
                session,
                url,
                retry_attempts=3,
                emit_log=False,  # Don't emit logs because the key is in the URL
            )

            log.info("fetched_current_prices", token_count=len(token_ids))

            # Parse response into records
            records = []
            current_time = datetime.now().isoformat()

            if "coins" in response:
                for formatted_token_id, price_info in response["coins"].items():
                    if price_info and "price" in price_info:
                        # Map back from formatted token ID to original token ID
                        original_token_id = formatted_token_id
                        if formatted_token_id.startswith("coingecko:"):
                            original_token_id = formatted_token_id[
                                10:
                            ]  # Remove "coingecko:" prefix

                        records.append(
                            {
                                "token_id": original_token_id,
                                "dt": current_time[:10],  # YYYY-MM-DD format
                                "price_usd": float(price_info["price"]),
                                "last_updated": current_time,
                            }
                        )

            df = pl.DataFrame(records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            return cls(df=df)

        except Exception as e:
            log.error("failed_to_fetch_current_prices", error=str(e))
            return cls(df=pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA))

    @classmethod
    def fetch_prices_at_timestamp(
        cls,
        token_ids: list[str],
        timestamp: int,
        session: requests.Session | None = None,
    ) -> "DefiLlamaTokenPrices":
        """Fetch prices at a specific timestamp.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            timestamp: Unix timestamp to get prices for
            session: Optional requests session

        Returns:
            DefiLlamaTokenPrices instance with price data at the specified timestamp
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # Format token IDs for DeFiLlama
        formatted_tokens = [format_token_id_for_defillama(tid) for tid in token_ids]
        tokens_str = ",".join(formatted_tokens)

        url = COINS_PRICES_HISTORICAL_ENDPOINT.format(
            api_key=api_key, timestamp=timestamp, coins=tokens_str
        )

        try:
            response = safe_get_data(
                session,
                url,
                retry_attempts=3,
                emit_log=False,  # Don't emit logs because the key is in the URL
            )

            log.info("fetched_prices_at_timestamp", token_count=len(token_ids), timestamp=timestamp)

            # Parse response into records
            records = []

            if "coins" in response:
                for formatted_token_id, price_info in response["coins"].items():
                    if price_info and "price" in price_info:
                        # Map back from formatted token ID to original token ID
                        original_token_id = formatted_token_id
                        if formatted_token_id.startswith("coingecko:"):
                            original_token_id = formatted_token_id[
                                10:
                            ]  # Remove "coingecko:" prefix

                        records.append(
                            {
                                "token_id": original_token_id,
                                "dt": dt_fromepoch(timestamp),
                                "price_usd": float(price_info["price"]),
                                "last_updated": datetime.now().isoformat(),
                            }
                        )

            df = pl.DataFrame(records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            return cls(df=df)

        except Exception as e:
            log.error("failed_to_fetch_prices_at_timestamp", error=str(e))
            return cls(df=pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA))

    @classmethod
    def fetch_prices_historical(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
        time_chunk_days: int = 7,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices for tokens.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            start_timestamp: Unix timestamp of earliest data point (optional)
            end_timestamp: Unix timestamp of latest data point (optional)
            span: Number of data points returned, defaults to 0 (all available)
            period: Duration between data points (e.g., '1d', '1h', '1w')
            search_width: Time range on either side to find price data
            session: Optional requests session
            time_chunk_days: Number of days per API call chunk (default: 7)

        Returns:
            DefiLlamaTokenPrices instance with historical price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # If we have a span and it's large, or if we have a large time range, chunk the requests
        should_chunk = False
        if start_timestamp and end_timestamp:
            days_range = (end_timestamp - start_timestamp) / (24 * 3600)
            should_chunk = days_range > time_chunk_days
        elif span > time_chunk_days:
            should_chunk = True

        if should_chunk:
            log.info(
                "chunking_historical_request",
                token_count=len(token_ids),
                time_chunk_days=time_chunk_days,
                span=span,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )

            return cls._fetch_prices_historical_chunked(
                token_ids=token_ids,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                span=span,
                period=period,
                search_width=search_width,
                session=session,
                time_chunk_days=time_chunk_days,
            )

        # For small requests, use the original single-call approach
        return cls._fetch_prices_historical_single(
            token_ids=token_ids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            span=span,
            period=period,
            search_width=search_width,
            session=session,
        )

    @classmethod
    def _fetch_prices_historical_single(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices for tokens in a single API call (original implementation)."""
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # Format token IDs for DeFiLlama
        formatted_tokens = [format_token_id_for_defillama(tid) for tid in token_ids]
        tokens_str = ",".join(formatted_tokens)

        # Build URL with parameters
        url = COINS_CHART_ENDPOINT.format(api_key=api_key, coins=tokens_str)

        params = {}
        if start_timestamp is not None:
            params["start"] = start_timestamp
        # Only include end_timestamp if start_timestamp is not provided
        # DeFiLlama API doesn't support both start and end parameters together
        if end_timestamp is not None and start_timestamp is None:
            params["end"] = end_timestamp
        if span > 0:
            params["span"] = span
        if period != "1d":
            params["period"] = period
        if search_width is not None:
            params["searchWidth"] = search_width

        try:
            response = safe_get_data(
                session,
                url,
                params=params,
                retry_attempts=3,
                emit_log=False,  # Don't emit logs because the key is in the URL
            )

            log.info("fetched_historical_prices_single", token_count=len(token_ids), params=params)

            # Parse response into records
            records = []

            if "coins" in response:
                for formatted_token_id, price_data in response["coins"].items():
                    if price_data and "prices" in price_data:
                        # Map back from formatted token ID to original token ID
                        original_token_id = formatted_token_id
                        if formatted_token_id.startswith("coingecko:"):
                            original_token_id = formatted_token_id[
                                10:
                            ]  # Remove "coingecko:" prefix

                        for price_point in price_data["prices"]:
                            if (
                                price_point
                                and "timestamp" in price_point
                                and "price" in price_point
                            ):
                                records.append(
                                    {
                                        "token_id": original_token_id,
                                        "dt": dt_fromepoch(price_point["timestamp"]),
                                        "price_usd": float(price_point["price"]),
                                        "last_updated": datetime.now().isoformat(),
                                    }
                                )

            df = pl.DataFrame(records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            return cls(df=df)

        except Exception as e:
            log.error(
                "failed_to_fetch_historical_prices_single", error=mask_api_key_in_text(str(e))
            )
            return cls(df=pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA))

    @classmethod
    def _fetch_prices_historical_chunked(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
        time_chunk_days: int = 7,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices for tokens using time-based chunking."""
        session = session or new_session()

        # Calculate the actual time range to chunk
        if start_timestamp and end_timestamp:
            # Use provided timestamps
            actual_start = start_timestamp
            actual_end = end_timestamp
            total_days = (actual_end - actual_start) / (24 * 3600)
        elif start_timestamp and span > 0:
            # Start timestamp + span days
            actual_start = start_timestamp
            actual_end = start_timestamp + (int(span) * 24 * 3600)
            total_days = int(span)
        elif end_timestamp and span > 0:
            # End timestamp - span days
            actual_start = end_timestamp - (int(span) * 24 * 3600)
            actual_end = end_timestamp
            total_days = int(span)
        else:
            # Default to last 30 days if no clear range
            actual_end = int(datetime.now().timestamp())
            actual_start = actual_end - (30 * 24 * 3600)
            total_days = 30

        # Create time chunks using only start timestamps
        chunk_size_seconds = time_chunk_days * 24 * 3600
        time_chunks = []

        current_start = actual_start
        remaining_days = total_days

        while remaining_days > 0:
            # For each chunk, we'll use start timestamp + span (days to fetch)
            chunk_days = min(time_chunk_days, remaining_days)
            time_chunks.append((current_start, chunk_days))

            # Move to next chunk: add chunk_days + 1 day buffer to avoid overlap issues
            current_start += int((chunk_days + 1) * 24 * 3600)
            remaining_days -= chunk_days

        log.info(
            "processing_time_chunks",
            total_chunks=len(time_chunks),
            chunk_size_days=time_chunk_days,
            total_days_requested=total_days,
        )

        # Fetch data for each time chunk
        all_records = []
        for chunk_num, (chunk_start, chunk_span) in enumerate(time_chunks, 1):
            log.info(
                "fetching_time_chunk",
                chunk=chunk_num,
                total_chunks=len(time_chunks),
                chunk_start=dt_fromepoch(chunk_start),
                chunk_span_days=chunk_span,
            )

            try:
                # Use only start timestamp and span for this chunk
                chunk_data = cls._fetch_prices_historical_single(
                    token_ids=token_ids,
                    start_timestamp=chunk_start,
                    end_timestamp=None,  # Don't use end timestamp
                    span=int(chunk_span),
                    period=period,
                    search_width=search_width,
                    session=session,
                )

                if not chunk_data.is_empty():
                    # Convert to records to combine later
                    chunk_records = chunk_data.df.to_dicts()
                    all_records.extend(chunk_records)

                log.info(
                    "completed_time_chunk",
                    chunk=chunk_num,
                    total_chunks=len(time_chunks),
                    prices_fetched=len(chunk_data.df) if not chunk_data.is_empty() else 0,
                )

            except Exception as e:
                log.error(
                    "failed_time_chunk",
                    chunk=chunk_num,
                    total_chunks=len(time_chunks),
                    error=mask_api_key_in_text(str(e)),
                )
                continue

        log.info("completed_chunked_historical_fetch", total_prices=len(all_records))

        # Combine all records and remove duplicates
        if all_records:
            df = pl.DataFrame(all_records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            # Remove duplicates based on token_id and dt
            df = df.unique(subset=["token_id", "dt"])
            # Sort by token_id and dt
            df = df.sort(["token_id", "dt"])

            # Filter to the original requested time range to remove excess data from buffer
            if start_timestamp and end_timestamp:
                start_date = dt_fromepoch(start_timestamp)
                end_date = dt_fromepoch(end_timestamp)
                df = df.filter((pl.col("dt") >= start_date) & (pl.col("dt") <= end_date))
        else:
            df = pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA)

        return cls(df=df)

    @classmethod
    def fetch_prices_historical_with_progress(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
        batch_size: int = 50,
        time_chunk_days: int = 7,
    ) -> "DefiLlamaTokenPrices":
        """
        Fetch historical prices for tokens with progress logging and time-based chunking.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            start_timestamp: Unix timestamp of earliest data point (optional)
            end_timestamp: Unix timestamp of latest data point (optional)
            span: Number of data points returned, defaults to 0 (all available)
            period: Duration between data points (e.g., '1d', '1h', '1w')
            search_width: Time range on either side to find price data
            session: Optional requests session
            batch_size: Number of tokens to process per batch
            time_chunk_days: Number of days per API call chunk (default: 7)

        Returns:
            DefiLlamaTokenPrices instance with historical price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # Determine if we need time chunking
        should_chunk = False
        if start_timestamp and end_timestamp:
            days_range = (end_timestamp - start_timestamp) / (24 * 3600)
            should_chunk = days_range > time_chunk_days
        elif span > time_chunk_days:
            should_chunk = True

        if should_chunk:
            log.info(
                "using_time_chunking_with_progress",
                token_count=len(token_ids),
                time_chunk_days=time_chunk_days,
                batch_size=batch_size,
            )

            return cls._fetch_prices_historical_chunked_with_progress(
                token_ids=token_ids,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                span=span,
                period=period,
                search_width=search_width,
                session=session,
                batch_size=batch_size,
                time_chunk_days=time_chunk_days,
            )

        # For small requests, use the original batch-based approach
        return cls._fetch_prices_historical_batched_only(
            token_ids=token_ids,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            span=span,
            period=period,
            search_width=search_width,
            session=session,
            batch_size=batch_size,
        )

    @classmethod
    def _fetch_prices_historical_batched_only(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
        batch_size: int = 50,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices using only token batching (original progress implementation)."""
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        all_records = []
        total_tokens = len(token_ids)

        # Process tokens in batches
        for i in range(0, total_tokens, batch_size):
            batch_tokens = token_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_tokens + batch_size - 1) // batch_size

            log.info(
                "fetching_historical_prices_batch",
                batch=batch_num,
                total_batches=total_batches,
                tokens_in_batch=len(batch_tokens),
                progress=f"{min(i + batch_size, total_tokens)}/{total_tokens}",
            )

            # Format token IDs for DeFiLlama
            formatted_tokens = [format_token_id_for_defillama(tid) for tid in batch_tokens]
            tokens_str = ",".join(formatted_tokens)

            # Build URL with parameters
            url = COINS_CHART_ENDPOINT.format(api_key=api_key, coins=tokens_str)

            params = {}
            if start_timestamp is not None:
                params["start"] = start_timestamp
            if end_timestamp is not None:
                params["end"] = end_timestamp
            if span > 0:
                params["span"] = span
            if period != "1d":
                params["period"] = period
            if search_width is not None:
                params["searchWidth"] = search_width

            try:
                response = safe_get_data(
                    session,
                    url,
                    params=params,
                    retry_attempts=3,
                    emit_log=False,  # Don't emit logs because the key is in the URL
                )

                if "coins" in response:
                    for formatted_token_id, price_data in response["coins"].items():
                        if price_data and "prices" in price_data:
                            # Map back from formatted token ID to original token ID
                            original_token_id = formatted_token_id
                            if formatted_token_id.startswith("coingecko:"):
                                original_token_id = formatted_token_id[
                                    10:
                                ]  # Remove "coingecko:" prefix

                            for price_point in price_data["prices"]:
                                if (
                                    price_point
                                    and "timestamp" in price_point
                                    and "price" in price_point
                                ):
                                    all_records.append(
                                        {
                                            "token_id": original_token_id,
                                            "dt": dt_fromepoch(price_point["timestamp"]),
                                            "price_usd": float(price_point["price"]),
                                            "last_updated": datetime.now().isoformat(),
                                        }
                                    )

                log.info(
                    "completed_historical_prices_batch",
                    batch=batch_num,
                    total_batches=total_batches,
                    prices_fetched=len(response.get("coins", {})),
                )

            except Exception as e:
                log.error(
                    "failed_to_fetch_historical_prices_batch",
                    batch=batch_num,
                    total_batches=total_batches,
                    error=mask_api_key_in_text(str(e)),
                )
                continue

        log.info("completed_historical_prices_fetch", total_prices=len(all_records))

        df = pl.DataFrame(all_records, schema=TOKEN_PRICES_SCHEMA, strict=False)
        return cls(df=df)

    @classmethod
    def _fetch_prices_historical_chunked_with_progress(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
        batch_size: int = 50,
        time_chunk_days: int = 7,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices using both time chunking and token batching with progress logging."""
        session = session or new_session()

        # Calculate the actual time range to chunk
        if start_timestamp and end_timestamp:
            actual_start = start_timestamp
            actual_end = end_timestamp
            total_days = (actual_end - actual_start) / (24 * 3600)
        elif start_timestamp and span > 0:
            actual_start = start_timestamp
            actual_end = start_timestamp + (int(span) * 24 * 3600)
            total_days = int(span)
        elif end_timestamp and span > 0:
            actual_start = end_timestamp - (int(span) * 24 * 3600)
            actual_end = end_timestamp
            total_days = int(span)
        else:
            actual_end = int(datetime.now().timestamp())
            actual_start = actual_end - (30 * 24 * 3600)
            total_days = 30

        # Create time chunks using only start timestamps
        chunk_size_seconds = time_chunk_days * 24 * 3600
        time_chunks = []

        current_start = actual_start
        remaining_days = total_days

        while remaining_days > 0:
            # For each chunk, we'll use start timestamp + span (days to fetch)
            chunk_days = min(time_chunk_days, remaining_days)
            time_chunks.append((current_start, chunk_days))

            # Move to next chunk: add chunk_days + 1 day buffer to avoid overlap issues
            current_start += int((chunk_days + 1) * 24 * 3600)
            remaining_days -= chunk_days

        log.info(
            "processing_time_chunks_with_progress",
            total_time_chunks=len(time_chunks),
            chunk_size_days=time_chunk_days,
            total_days_requested=total_days,
            token_count=len(token_ids),
            batch_size=batch_size,
        )

        # Fetch data for each time chunk
        all_records = []
        for chunk_num, (chunk_start, chunk_span) in enumerate(time_chunks, 1):
            log.info(
                "processing_time_chunk_with_batches",
                time_chunk=chunk_num,
                total_time_chunks=len(time_chunks),
                chunk_start=dt_fromepoch(chunk_start),
                chunk_span_days=chunk_span,
            )

            try:
                # Use batched approach for this time chunk
                chunk_data = cls._fetch_prices_historical_batched_only(
                    token_ids=token_ids,
                    start_timestamp=chunk_start,
                    end_timestamp=None,  # Don't use end timestamp
                    span=int(chunk_span),
                    period=period,
                    search_width=search_width,
                    session=session,
                    batch_size=batch_size,
                )

                if not chunk_data.is_empty():
                    chunk_records = chunk_data.df.to_dicts()
                    all_records.extend(chunk_records)

                log.info(
                    "completed_time_chunk_with_batches",
                    time_chunk=chunk_num,
                    total_time_chunks=len(time_chunks),
                    prices_fetched=len(chunk_data.df) if not chunk_data.is_empty() else 0,
                )

            except Exception as e:
                log.error(
                    "failed_time_chunk_with_batches",
                    time_chunk=chunk_num,
                    total_time_chunks=len(time_chunks),
                    error=mask_api_key_in_text(str(e)),
                )
                continue

        log.info("completed_chunked_historical_fetch_with_progress", total_prices=len(all_records))

        # Combine all records and remove duplicates
        if all_records:
            df = pl.DataFrame(all_records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            # Remove duplicates based on token_id and dt
            df = df.unique(subset=["token_id", "dt"])
            # Sort by token_id and dt
            df = df.sort(["token_id", "dt"])

            # Filter to the original requested time range to remove excess data from buffer
            if start_timestamp and end_timestamp:
                start_date = dt_fromepoch(start_timestamp)
                end_date = dt_fromepoch(end_timestamp)
                df = df.filter((pl.col("dt") >= start_date) & (pl.col("dt") <= end_date))
        else:
            df = pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA)

        return cls(df=df)

    @classmethod
    def fetch_prices_by_days(
        cls,
        token_ids: list[str],
        days: int = 30,
        session: requests.Session | None = None,
        time_chunk_days: int = 7,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices for the last N days.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            days: Number of days of historical data to fetch
            session: Optional requests session
            time_chunk_days: Number of days per API call chunk (default: 7)

        Returns:
            DefiLlamaTokenPrices instance with historical price data
        """
        # Calculate start timestamp for N days ago
        start_date = datetime.now() - timedelta(days=days)
        start_timestamp = int(start_date.timestamp())

        return cls.fetch_prices_historical(
            token_ids=token_ids,
            start_timestamp=start_timestamp,
            span=days,  # Return daily data points for the specified number of days
            period="1d",
            session=session,
            time_chunk_days=time_chunk_days,
        )

    @classmethod
    def fetch_first_prices(
        cls,
        token_ids: list[str],
        session: requests.Session | None = None,
    ) -> "DefiLlamaTokenPrices":
        """Fetch the first recorded prices for tokens.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            session: Optional requests session

        Returns:
            DefiLlamaTokenPrices instance with first price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # Format token IDs for DeFiLlama
        formatted_tokens = [format_token_id_for_defillama(tid) for tid in token_ids]
        tokens_str = ",".join(formatted_tokens)

        # Use chart endpoint with early start timestamp and span=1 to get first price
        url = COINS_CHART_ENDPOINT.format(api_key=api_key, coins=tokens_str)

        # Set parameters to get the earliest available data point
        params = {
            "start": 1,  # Very early timestamp (Jan 1, 1970)
            "span": 1,  # Only get the first data point
        }

        try:
            response = safe_get_data(
                session,
                url,
                params=params,
                retry_attempts=3,
                emit_log=False,  # Don't emit logs because the key is in the URL
            )

            log.info("fetched_first_prices", token_count=len(token_ids))

            # Parse response into records
            records = []

            if "coins" in response:
                for formatted_token_id, price_data in response["coins"].items():
                    if price_data and "prices" in price_data and price_data["prices"]:
                        # Map back from formatted token ID to original token ID
                        original_token_id = formatted_token_id
                        if formatted_token_id.startswith("coingecko:"):
                            original_token_id = formatted_token_id[
                                10:
                            ]  # Remove "coingecko:" prefix

                        # Get the first (earliest) price point
                        first_price = price_data["prices"][0]
                        if first_price and "timestamp" in first_price and "price" in first_price:
                            records.append(
                                {
                                    "token_id": original_token_id,
                                    "dt": dt_fromepoch(first_price["timestamp"]),
                                    "price_usd": float(first_price["price"]),
                                    "last_updated": datetime.now().isoformat(),
                                }
                            )

            df = pl.DataFrame(records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            return cls(df=df)

        except Exception as e:
            log.error("failed_to_fetch_first_prices", error=str(e))
            return cls(df=pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA))

    @classmethod
    def fetch_prices_current_with_progress(
        cls,
        token_ids: list[str],
        session: requests.Session | None = None,
        batch_size: int = 50,
    ) -> "DefiLlamaTokenPrices":
        """
        Fetch current prices for tokens with progress logging.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            session: Optional requests session
            batch_size: Number of tokens to process per batch

        Returns:
            DefiLlamaTokenPrices instance with current price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        all_records = []
        total_tokens = len(token_ids)

        # Process tokens in batches
        for i in range(0, total_tokens, batch_size):
            batch_tokens = token_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_tokens + batch_size - 1) // batch_size

            log.info(
                "fetching_current_prices_batch",
                batch=batch_num,
                total_batches=total_batches,
                tokens_in_batch=len(batch_tokens),
                progress=f"{min(i + batch_size, total_tokens)}/{total_tokens}",
            )

            # Format token IDs for DeFiLlama
            formatted_tokens = [format_token_id_for_defillama(tid) for tid in batch_tokens]
            tokens_str = ",".join(formatted_tokens)

            url = COINS_PRICES_CURRENT_ENDPOINT.format(api_key=api_key, coins=tokens_str)

            try:
                response = safe_get_data(
                    session,
                    url,
                    retry_attempts=3,
                    emit_log=False,  # Don't emit logs because the key is in the URL
                )

                current_time = datetime.now().isoformat()

                if "coins" in response:
                    for formatted_token_id, price_info in response["coins"].items():
                        if price_info and "price" in price_info:
                            # Map back from formatted token ID to original token ID
                            original_token_id = formatted_token_id
                            if formatted_token_id.startswith("coingecko:"):
                                original_token_id = formatted_token_id[
                                    10:
                                ]  # Remove "coingecko:" prefix

                            all_records.append(
                                {
                                    "token_id": original_token_id,
                                    "dt": current_time[:10],  # YYYY-MM-DD format
                                    "price_usd": float(price_info["price"]),
                                    "last_updated": current_time,
                                }
                            )

                log.info(
                    "completed_current_prices_batch",
                    batch=batch_num,
                    total_batches=total_batches,
                    prices_fetched=len(response.get("coins", {})),
                )

            except Exception as e:
                log.error(
                    "failed_to_fetch_current_prices_batch",
                    batch=batch_num,
                    total_batches=total_batches,
                    error=mask_api_key_in_text(str(e)),
                )
                continue

        log.info("completed_current_prices_fetch", total_prices=len(all_records))

        df = pl.DataFrame(all_records, schema=TOKEN_PRICES_SCHEMA, strict=False)
        return cls(df=df)

    @classmethod
    def fetch_first_prices_with_progress(
        cls,
        token_ids: list[str],
        session: requests.Session | None = None,
        batch_size: int = 50,
    ) -> "DefiLlamaTokenPrices":
        """
        Fetch the first recorded prices for tokens with progress logging.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            session: Optional requests session
            batch_size: Number of tokens to process per batch

        Returns:
            DefiLlamaTokenPrices instance with first price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        all_records = []
        total_tokens = len(token_ids)

        # Process tokens in batches
        for i in range(0, total_tokens, batch_size):
            batch_tokens = token_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_tokens + batch_size - 1) // batch_size

            log.info(
                "fetching_first_prices_batch",
                batch=batch_num,
                total_batches=total_batches,
                tokens_in_batch=len(batch_tokens),
                progress=f"{min(i + batch_size, total_tokens)}/{total_tokens}",
            )

            # Format token IDs for DeFiLlama
            formatted_tokens = [format_token_id_for_defillama(tid) for tid in batch_tokens]
            tokens_str = ",".join(formatted_tokens)

            # Use chart endpoint with early start timestamp and span=1 to get first price
            url = COINS_CHART_ENDPOINT.format(api_key=api_key, coins=tokens_str)

            # Set parameters to get the earliest available data point
            params = {
                "start": 1,  # Very early timestamp (Jan 1, 1970)
                "span": 1,  # Only get the first data point
            }

            try:
                response = safe_get_data(
                    session,
                    url,
                    params=params,
                    retry_attempts=3,
                    emit_log=False,  # Don't emit logs because the key is in the URL
                )

                if "coins" in response:
                    for formatted_token_id, price_data in response["coins"].items():
                        if price_data and "prices" in price_data and price_data["prices"]:
                            # Map back from formatted token ID to original token ID
                            original_token_id = formatted_token_id
                            if formatted_token_id.startswith("coingecko:"):
                                original_token_id = formatted_token_id[
                                    10:
                                ]  # Remove "coingecko:" prefix

                            # Get the first (earliest) price point
                            first_price = price_data["prices"][0]
                            if (
                                first_price
                                and "timestamp" in first_price
                                and "price" in first_price
                            ):
                                all_records.append(
                                    {
                                        "token_id": original_token_id,
                                        "dt": dt_fromepoch(first_price["timestamp"]),
                                        "price_usd": float(first_price["price"]),
                                        "last_updated": datetime.now().isoformat(),
                                    }
                                )

                log.info(
                    "completed_first_prices_batch",
                    batch=batch_num,
                    total_batches=total_batches,
                    prices_fetched=len(response.get("coins", {})),
                )

            except Exception as e:
                log.error(
                    "failed_to_fetch_first_prices_batch",
                    batch=batch_num,
                    total_batches=total_batches,
                    error=mask_api_key_in_text(str(e)),
                )
                continue

        log.info("completed_first_prices_fetch", total_prices=len(all_records))

        df = pl.DataFrame(all_records, schema=TOKEN_PRICES_SCHEMA, strict=False)
        return cls(df=df)

    def is_empty(self) -> bool:
        """Check if the dataframe is empty."""
        return self.df.is_empty()

    def __len__(self) -> int:
        """Return the number of records."""
        return len(self.df)
