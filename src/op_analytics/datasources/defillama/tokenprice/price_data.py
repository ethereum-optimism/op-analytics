from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl
import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import dt_fromepoch

log = structlog.get_logger()

# DeFiLlama Pro API endpoints
COINS_CHART_ENDPOINT = "https://pro-api.llama.fi/{api_key}/coins/chart/{coins}"
COINS_PRICES_ENDPOINT = "https://pro-api.llama.fi/{api_key}/coins/prices/{coins}"
COINS_FIRST_PRICES_ENDPOINT = "https://pro-api.llama.fi/{api_key}/coins/first/{coins}"


TOKEN_PRICES_SCHEMA = pl.Schema(
    [
        ("token_id", pl.String),
        ("dt", pl.String),
        ("price_usd", pl.Float64),
        ("last_updated", pl.String),
    ]
)


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

        # Join token IDs with commas
        tokens_str = ",".join(token_ids)

        url = COINS_PRICES_ENDPOINT.format(api_key=api_key, coins=tokens_str)

        try:
            response = get_data(
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
                for token_id, price_info in response["coins"].items():
                    if price_info and "price" in price_info:
                        records.append(
                            {
                                "token_id": token_id,
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
    def fetch_prices_historical(
        cls,
        token_ids: list[str],
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        span: int = 0,
        period: str = "1d",
        search_width: str | None = None,
        session: requests.Session | None = None,
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

        Returns:
            DefiLlamaTokenPrices instance with historical price data
        """
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        # Join token IDs with commas
        tokens_str = ",".join(token_ids)

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
            response = get_data(
                session,
                url,
                params=params,
                retry_attempts=3,
                emit_log=False,  # Don't emit logs because the key is in the URL
            )

            log.info("fetched_historical_prices", token_count=len(token_ids), params=params)

            # Parse response into records
            records = []

            if "coins" in response:
                for token_id, price_data in response["coins"].items():
                    if price_data and "prices" in price_data:
                        for price_point in price_data["prices"]:
                            if (
                                price_point
                                and "timestamp" in price_point
                                and "price" in price_point
                            ):
                                records.append(
                                    {
                                        "token_id": token_id,
                                        "dt": dt_fromepoch(price_point["timestamp"]),
                                        "price_usd": float(price_point["price"]),
                                        "last_updated": datetime.now().isoformat(),
                                    }
                                )

            df = pl.DataFrame(records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            return cls(df=df)

        except Exception as e:
            log.error("failed_to_fetch_historical_prices", error=str(e))
            return cls(df=pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA))

    @classmethod
    def fetch_prices_by_days(
        cls,
        token_ids: list[str],
        days: int = 30,
        session: requests.Session | None = None,
    ) -> "DefiLlamaTokenPrices":
        """Fetch historical prices for the last N days.

        Args:
            token_ids: List of token IDs (coingecko slugs or chain:address format)
            days: Number of days of historical data to fetch
            session: Optional requests session

        Returns:
            DefiLlamaTokenPrices instance with historical price data
        """
        # Calculate start timestamp for N days ago
        start_date = datetime.now() - timedelta(days=days)
        start_timestamp = int(start_date.timestamp())

        return cls.fetch_prices_historical(
            token_ids=token_ids,
            start_timestamp=start_timestamp,
            period="1d",
            session=session,
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

        # Join token IDs with commas
        tokens_str = ",".join(token_ids)

        url = COINS_FIRST_PRICES_ENDPOINT.format(api_key=api_key, coins=tokens_str)

        try:
            response = get_data(
                session,
                url,
                retry_attempts=3,
                emit_log=False,  # Don't emit logs because the key is in the URL
            )

            log.info("fetched_first_prices", token_count=len(token_ids))

            # Parse response into records
            records = []

            if "coins" in response:
                for token_id, price_info in response["coins"].items():
                    if price_info and "price" in price_info and "timestamp" in price_info:
                        records.append(
                            {
                                "token_id": token_id,
                                "dt": dt_fromepoch(price_info["timestamp"]),
                                "price_usd": float(price_info["price"]),
                                "last_updated": datetime.now().isoformat(),
                            }
                        )

            df = pl.DataFrame(records, schema=TOKEN_PRICES_SCHEMA, strict=False)
            return cls(df=df)

        except Exception as e:
            log.error("failed_to_fetch_first_prices", error=str(e))
            return cls(df=pl.DataFrame([], schema=TOKEN_PRICES_SCHEMA))

    def is_empty(self) -> bool:
        """Check if the dataframe is empty."""
        return self.df.is_empty()

    def __len__(self) -> int:
        """Return the number of records."""
        return len(self.df)
