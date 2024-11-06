from dataclasses import dataclass
from typing import Optional

import polars as pl
from op_coreutils.bigquery.write import (
    most_recent_dates,
    upsert_partitioned_table,
    upsert_unpartitioned_table,
)
from op_coreutils.logger import structlog
from op_coreutils.request import get_data, new_session
from op_coreutils.threads import run_concurrently
from op_coreutils.time import dt_fromepoch

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
BREAKDOWN_ENDPOINT = "https://stablecoins.llama.fi/stablecoin/{id}"

BQ_DATASET = "uploads_api"

METADATA_TABLE = "defillama_stablecoin_metadata"
BALANCES_TABLE = "defillama_daily_stablecoin_balances"
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

    # Upsert metadata to BQ.
    upsert_unpartitioned_table(
        df=result.metadata_df,
        dataset=BQ_DATASET,
        table_name=METADATA_TABLE,
        unique_keys=["id", "name", "symbol"],
    )

    # Upsert balances to BQ.
    # Only update balances on the most recent dates. Assume data further back in time is immutable.
    upsert_partitioned_table(
        df=most_recent_dates(result.balances_df, n_dates=BALANCES_TABLE_LAST_N_DAYS),
        dataset=BQ_DATASET,
        table_name=BALANCES_TABLE,
        unique_keys=["dt", "id", "chain"],
    )

    return result


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

    return DefillamaStablecoins(
        metadata_df=pl.DataFrame(metadata, infer_schema_length=len(metadata)),
        balances_df=pl.DataFrame(balances, infer_schema_length=len(balances)),
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

    return metadata


def single_stablecoin_balances(data: dict) -> tuple[dict, list[dict]]:
    """Extract balances for a single stablecoin.

    Args:
        data: Data for this stablecoin as returned by the API

    Returns:
        A list of rows. Each row is information at a point in time.
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
                "circulating": get_value(datapoint, "circulating"),
                "bridged_to": get_value(datapoint, "bridgedTo"),
                "minted": get_value(datapoint, "minted"),
                "unreleased": get_value(datapoint, "unreleased"),
                "name": metadata["name"],
                "symbol": metadata["symbol"],
            }
            balances.append(row)

    return metadata, balances
