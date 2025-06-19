"""
CoinGecko price data source for fetching daily token prices.
"""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import polars as pl
import requests
import stamina
from requests.exceptions import JSONDecodeError

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.env.vault import env_get

log = structlog.get_logger()

COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_RATE_LIMIT = 5  # requests per minute (very conservative)
COINGECKO_RETRY_ATTEMPTS = 3
COINGECKO_RETRY_WAIT = 60  # seconds (1 minute)


class CoinGeckoRateLimit(Exception):
    """Raised when a rate limit error is encountered from the CoinGecko API."""

    pass


class CoinGeckoResponseError(Exception):
    """Raised when the CoinGecko API response is invalid."""

    pass


class CoinGeckoTokenNotFound(Exception):
    """Raised when a token ID is not found in CoinGecko."""

    pass


@dataclass
class TokenPriceData:
    """Represents daily price data for a token from CoinGecko."""

    token_id: str
    dt: str  # ISO format date string
    price_usd: float
    market_cap_usd: Optional[float]
    total_volume_usd: Optional[float]
    last_updated: str  # ISO format timestamp

    @classmethod
    def from_api_response(
        cls,
        token_data: dict,
        timestamp: int,
        price: float,
        market_cap: Optional[float],
        volume: Optional[float],
    ) -> "TokenPriceData":
        """Create a TokenPriceData instance from API response data."""
        date = datetime.fromtimestamp(timestamp / 1000)
        return cls(
            token_id=token_data["id"],
            dt=date.date().isoformat(),
            price_usd=price,
            market_cap_usd=market_cap,
            total_volume_usd=volume,
            last_updated=datetime.now().isoformat(),
        )


class CoinGeckoDataSource:
    """Data source for fetching token price data from CoinGecko."""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or new_session()
        self._last_request_time = 0

        # Try to get API key, but don't fail if it doesn't exist
        try:
            self._api_key = env_get("COINGECKO_API_KEY")
            log.info("Using CoinGecko Pro API")
        except KeyError:
            self._api_key = None
            log.info("Using CoinGecko Free API (rate limited)")

        # Set up headers
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["x-cg-pro-api-key"] = self._api_key
        self.session.headers.update(headers)

    def _rate_limit(self):
        """Implement rate limiting for CoinGecko API."""
        now = time.time()
        time_since_last = now - self._last_request_time
        if time_since_last < (60 / COINGECKO_RATE_LIMIT):
            time.sleep((60 / COINGECKO_RATE_LIMIT) - time_since_last)
        self._last_request_time = time.time()

    @stamina.retry(
        on=CoinGeckoRateLimit, attempts=COINGECKO_RETRY_ATTEMPTS, wait_initial=COINGECKO_RETRY_WAIT
    )
    def get_token_prices(
        self,
        token_ids: List[str],
        days: int,
        vs_currency: str = "usd",
    ) -> pl.DataFrame:
        """
        Fetch daily price data for a list of tokens from CoinGecko.

        Args:
            token_ids: List of CoinGecko token IDs
            days: Number of days of historical data to fetch
            vs_currency: Currency to fetch prices in (default: usd)

        Returns:
            Polars DataFrame with price data
        """
        self._rate_limit()

        # CoinGecko API has a limit of 100 tokens per request
        all_price_data = []
        failed_tokens = []

        for i in range(0, len(token_ids), 100):
            batch = token_ids[i : i + 100]
            ids_param = ",".join(batch)

            # Use different endpoints based on whether we have an API key
            if self._api_key:
                url = f"{COINGECKO_API_BASE}/coins/markets"
                params = {
                    "vs_currency": vs_currency,
                    "ids": ids_param,
                    "days": str(days),
                    "interval": "daily",
                }
            else:
                # For free tier, we need to fetch each coin individually
                url = f"{COINGECKO_API_BASE}/coins/{batch[0]}/market_chart"
                params = {
                    "vs_currency": vs_currency,
                    "days": str(days),
                    "interval": "daily",
                }

            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if not data:
                    log.warning("No data returned for tokens", tokens=batch)
                    continue

                log.info(
                    "Received data for tokens", count=len(data) if isinstance(data, list) else 1
                )

                # Handle different response formats for free vs pro API
                if self._api_key:
                    for token_data in data:
                        prices = token_data.get("prices", [])
                        market_caps = token_data.get("market_caps", [])
                        volumes = token_data.get("total_volumes", [])

                        for idx, (timestamp, price) in enumerate(prices):
                            market_cap = market_caps[idx][1] if idx < len(market_caps) else None
                            volume = volumes[idx][1] if idx < len(volumes) else None

                            price_data = TokenPriceData.from_api_response(
                                token_data={"id": token_data["id"]},
                                timestamp=timestamp,
                                price=price,
                                market_cap=market_cap,
                                volume=volume,
                            )
                            all_price_data.append(price_data)
                else:
                    # Free API response format
                    prices = data.get("prices", [])
                    market_caps = data.get("market_caps", [])
                    volumes = data.get("total_volumes", [])

                    for idx, (timestamp, price) in enumerate(prices):
                        market_cap = market_caps[idx][1] if idx < len(market_caps) else None
                        volume = volumes[idx][1] if idx < len(volumes) else None

                        price_data = TokenPriceData.from_api_response(
                            token_data={"id": batch[0]},
                            timestamp=timestamp,
                            price=price,
                            market_cap=market_cap,
                            volume=volume,
                        )
                        all_price_data.append(price_data)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    raise CoinGeckoRateLimit(f"Rate limit exceeded: {e}")
                elif e.response.status_code == 404:  # Token not found
                    failed_token = batch[0] if not self._api_key else None
                    if failed_token:
                        log.warning("Token not found in CoinGecko", token_id=failed_token)
                        failed_tokens.append(failed_token)
                    continue
                log.error("API request failed", error=str(e), status_code=e.response.status_code)
                raise
            except JSONDecodeError as e:
                log.error("Invalid JSON response", error=str(e))
                raise CoinGeckoResponseError(f"Invalid JSON response: {e}")

        if failed_tokens:
            log.warning("Failed to fetch data for tokens", failed_tokens=failed_tokens)

        # Convert to DataFrame
        df = pl.DataFrame([vars(data) for data in all_price_data])
        return df

    def get_latest_prices(
        self,
        token_ids: List[str],
        vs_currency: str = "usd",
    ) -> pl.DataFrame:
        """
        Fetch latest price data for a list of tokens from CoinGecko.

        Args:
            token_ids: List of CoinGecko token IDs
            vs_currency: Currency to fetch prices in (default: usd)

        Returns:
            Polars DataFrame with price data
        """
        return self.get_token_prices(token_ids, days=1, vs_currency=vs_currency)
