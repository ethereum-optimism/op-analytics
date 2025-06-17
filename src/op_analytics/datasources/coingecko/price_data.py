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

log = structlog.get_logger()

COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_RATE_LIMIT = 50  # requests per minute
COINGECKO_RETRY_ATTEMPTS = 3
COINGECKO_RETRY_WAIT = 10  # seconds


class CoinGeckoRateLimit(Exception):
    """Raised when a rate limit error is encountered from the CoinGecko API."""

    pass


class CoinGeckoResponseError(Exception):
    """Raised when the CoinGecko API response is invalid."""

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
        for i in range(0, len(token_ids), 100):
            batch = token_ids[i : i + 100]
            ids_param = ",".join(batch)

            url = f"{COINGECKO_API_BASE}/coins/markets"
            params = {
                "vs_currency": vs_currency,
                "ids": ids_param,
                "days": str(days),
                "interval": "daily",
            }

            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    raise CoinGeckoRateLimit(f"Rate limit exceeded: {e}")
                raise
            except JSONDecodeError as e:
                raise CoinGeckoResponseError(f"Invalid JSON response: {e}")

            for token_data in data:
                prices = token_data.get("prices", [])
                market_caps = token_data.get("market_caps", [])
                volumes = token_data.get("total_volumes", [])

                for i, (timestamp, price) in enumerate(prices):
                    market_cap = market_caps[i][1] if i < len(market_caps) else None
                    volume = volumes[i][1] if i < len(volumes) else None

                    price_data = TokenPriceData.from_api_response(
                        token_data=token_data,
                        timestamp=timestamp,
                        price=price,
                        market_cap=market_cap,
                        volume=volume,
                    )
                    all_price_data.append(price_data)

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
