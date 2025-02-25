from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

import requests


from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import dt_fromepoch

BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"


MUST_HAVE_METADATA_FIELDS = [
    "id",
    "name",
    "address",
    "symbol",
    "url",
    "pegType",
    "pegMechanism",
]

OPTIONAL_METADATA_FIELDS = [
    "description",
    "mintRedeemDescription",
    "onCoinGecko",
    "gecko_id",
    "cmcId",
    "priceSource",
    "twitter",
    "price",
]


@dataclass
class StableCoin:
    """Historical data for a single stablecoin."""

    stablecoin_id: str
    metadata: dict[str, Any]
    balances: list[dict[str, Any]]

    @classmethod
    def fetch(cls, stablecoin_id: str, session: requests.Session | None = None) -> "StableCoin":
        session = session or new_session()

        url = BREAKDOWN_ENDPOINT.format(id=stablecoin_id)
        response = get_data(
            session,
            url,
            retry_attempts=3,
            emit_log=True,
        )

        return cls.of(
            stablecoin_id=stablecoin_id,
            data=response,
        )

    @classmethod
    def of(cls, stablecoin_id: str, data: dict[str, Any]) -> "StableCoin":
        assert stablecoin_id == data["id"]
        metadata_row, balance_rows = single_stablecoin_balances(data)

        if not metadata_row:
            raise ValueError(f"No metadata for stablecoin={data['name']}")

        if not balance_rows:
            raise ValueError(f"No balances for stablecoin={data['name']}")

        return cls(
            stablecoin_id=stablecoin_id,
            metadata=metadata_row,
            balances=balance_rows,
        )


def single_stablecoin_metadata(data: dict) -> dict:
    """Extract metadata for a single stablecoin.

    Will fail if the response data from the API is missing any of the "must-have"
    metadata fields.

    Args:
        data: Data for this stablecoin as returned by the API

    Returns:
        The metadata dictionary.
    """
    metadata: dict[str, Optional[str]] = {}

    # Collect required metadata fields
    for key in MUST_HAVE_METADATA_FIELDS:
        metadata[key] = data[key]

    # Collect additional optional metadata fields
    for key in OPTIONAL_METADATA_FIELDS:
        metadata[key] = data.get(key)

    # pedrod - 2025/01/24.
    # The API seems to have a problem where price is sometimes of type string.
    # For example (id='226', name='Frankencoin', price='1.1017182')
    if "price" in metadata and isinstance(metadata["price"], str):
        try:
            metadata["price"] = float(metadata["price"])  # type: ignore
        except ValueError:
            metadata["price"] = None

    return metadata


def safe_decimal(float_val):
    """Safely convert DefiLLama balance values to Decimal.

    Balance values can be int or float. This function converts values to Decimal,
    so we don't have to rely on polars schema inference.
    """
    if float_val is None:
        return None

    return Decimal(str(float_val))


def single_stablecoin_balances(data: dict) -> tuple[dict, list[dict]]:
    """Extract balances for a single stablecoin.

    Args:
        data: Data for this stablecoin as returned by the API

    Returns:
        Tuple of metadata dict and balances for this stablecoin.
        Each item in balances is one data point obtained from DefiLlama.
    """
    metadata = single_stablecoin_metadata(data)
    peg_type: str = data["pegType"]

    def get_value(_datapoint, _metric_name):
        """Helper to get a nested dict key with fallback."""
        return _datapoint.get(_metric_name, {}).get(peg_type)

    balances = []

    for chain, balance in data["chainBalances"].items():
        tokens = balance.get("tokens", [])

        for datapoint in tokens:
            row = {
                "id": data["id"],
                "chain": chain,
                "dt": dt_fromepoch(datapoint["date"]),
                "circulating": safe_decimal(get_value(datapoint, "circulating")),
                "bridged_to": safe_decimal(get_value(datapoint, "bridgedTo")),
                "minted": safe_decimal(get_value(datapoint, "minted")),
                "unreleased": safe_decimal(get_value(datapoint, "unreleased")),
                "name": metadata["name"],
                "symbol": metadata["symbol"],
            }
            balances.append(row)

    return metadata, balances
