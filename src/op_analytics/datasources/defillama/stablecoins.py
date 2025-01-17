import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import dt_fromepoch, now_dt

from .dataaccess import DefiLlama

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"


BALANCES_TABLE_LAST_N_DAYS = 7  # upsert only the last 7 days of balances fetched from the api


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

METADATA_DF_SCHEMA = {
    "id": pl.String,
    "name": pl.String,
    "address": pl.String,
    "symbol": pl.String,
    "url": pl.String,
    "pegType": pl.String,
    "pegMechanism": pl.String,
    "description": pl.String,
    "mintRedeemDescription": pl.String,
    "onCoinGecko": pl.String,
    "gecko_id": pl.String,
    "cmcId": pl.String,
    "priceSource": pl.String,
    "twitter": pl.String,
    "price": pl.Float64,
}

BALANCES_DF_SCHEMA = {
    "id": pl.String(),
    "chain": pl.String(),
    "dt": pl.String(),
    "circulating": pl.Decimal(precision=38, scale=18),
    "bridged_to": pl.Decimal(precision=38, scale=18),
    "minted": pl.Decimal(precision=38, scale=18),
    "unreleased": pl.Decimal(precision=38, scale=18),
    "name": pl.String(),
    "symbol": pl.String(),
}


@dataclass
class DefillamaStablecoins:
    """Metadata and balances for all stablecoins.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    metadata_df: pl.DataFrame
    balances_df: pl.DataFrame


def construct_urls(stablecoins_summary, symbols: list[str] | None) -> dict[str, str]:
    """Build the collection of urls that we will fetch from DefiLlama.

    Args:
        symbols: list of symbols to process. Defaults to None (process all).
    """
    urls = {}
    for stablecoin in stablecoins_summary:
        stablecoin_symbol = stablecoin["symbol"]
        stablecoin_id = stablecoin["id"]
        if symbols is None or stablecoin_symbol in symbols:
            urls[stablecoin_id] = BREAKDOWN_ENDPOINT.format(id=stablecoin_id)

    if not urls:
        raise ValueError("No valid stablecoin IDs provided.")
    return urls


def pull_stablecoins(symbols: list[str] | None = None) -> DefillamaStablecoins:
    """
    Pulls and processes stablecoin data from DeFiLlama.

    Args:
        stablecoin_ids: list of stablecoin IDs to process. Defaults to None (process all).
    """
    session = new_session()

    # Call the summary endpoint to find the list of stablecoins tracked by DefiLLama.
    summary = get_data(session, SUMMARY_ENDPOINT)
    stablecoins_summary = summary["peggedAssets"]

    # Call the API endpoint for each stablecoin in parallel.
    urls = construct_urls(stablecoins_summary, symbols=symbols)
    stablecoins_data = run_concurrently(lambda x: get_data(session, x), urls, max_workers=4)

    # Extract all the balances (includes metadata).
    result = extract(stablecoins_data)

    # Write metadata.
    DefiLlama.STABLECOINS_METADATA.write(
        dataframe=result.metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["symbol"],
    )

    # Write balances.
    DefiLlama.STABLECOINS_BALANCE.write(
        dataframe=most_recent_dates(result.balances_df, n_dates=BALANCES_TABLE_LAST_N_DAYS),
        sort_by=["symbol", "chain"],
    )

    return result


def execute_pull():
    result = pull_stablecoins()
    return {
        "metadata_df": dt_summary(result.metadata_df),
        "balances_df": dt_summary(result.balances_df),
    }


def extract(stablecoins_data) -> DefillamaStablecoins:
    """Extract metadata and balances for all stablecoins."""
    metadata: list[dict] = []
    balances: list[dict] = []
    for stablecoin_id, data in stablecoins_data.items():
        assert stablecoin_id == data["id"]
        metadata_row, balance_rows = single_stablecoin_balances(data)

        if not metadata_row:
            raise ValueError(f"No metadata for stablecoin={data['name']}")

        if not balance_rows:
            raise ValueError(f"No balances for stablecoin={data['name']}")

        metadata.append(metadata_row)
        balances.extend(balance_rows)

    metadata_df = pl.DataFrame(metadata, infer_schema_length=len(metadata))
    balances_df = pl.DataFrame(balances, schema=BALANCES_DF_SCHEMA)

    # Schema assertions to help our future selves reading this code.
    assert metadata_df.schema == METADATA_DF_SCHEMA
    if balances_df.schema != BALANCES_DF_SCHEMA:
        actual = json.dumps([f"{_}  {str(__)}" for _, __ in balances_df.schema.items()])
        expected = json.dumps([f"{_}  {str(__)}" for _, __ in BALANCES_DF_SCHEMA.items()])

        raise Exception(f"""
        Mismatching schema:
        
        {actual}
        
        {expected}
        """)

    assert balances_df.schema == BALANCES_DF_SCHEMA

    return DefillamaStablecoins(metadata_df=metadata_df, balances_df=balances_df)


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
