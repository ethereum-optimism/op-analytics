from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import polars as pl

from op_analytics.coreutils.bigquery.write import (
    most_recent_dates,
    upsert_partitioned_table,
    upsert_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import date_fromstr, dt_fromepoch, epoch_is_date, now_date

log = structlog.get_logger()

PROTOCOLS_ENDPOINT = "https://api.llama.fi/protocols"
PROTOCOL_DETAILS_ENDPOINT = "https://api.llama.fi/protocol/{slug}"

BQ_DATASET = "uploads_api"
PROTOCOL_METADATA_TABLE = "defillama_protocols_metadata"
PROTOCOL_TVL_DATA_TABLE = "defillama_protocols_tvl"
PROTOCOL_TOKEN_TVL_DATA_TABLE = "defillama_protocols_token_tvl"

TVL_TABLE_LAST_N_DAYS = 7

TVL_TABLE_CUTOFF_DATE = now_date() - timedelta(TVL_TABLE_LAST_N_DAYS)
EXCLUDE_CATEGORIES = ["CEX", "Chain"]

DUMMY_TVL_DF = pl.DataFrame(
    {
        "protocol_slug": [],
        "chain": [],
        "dt": [],
        "total_app_tvl": [],
    }
)
DUMMY_TOKEN_TVL_DF = pl.DataFrame(
    {
        "protocol_slug": [],
        "chain": [],
        "dt": [],
        "token": [],
        "app_token_tvl": [],
    }
)
DUMMY_TOKEN_USD_TVL_DF = pl.DataFrame(
    {
        "protocol_slug": [],
        "chain": [],
        "dt": [],
        "token": [],
        "app_token_tvl_usd": [],
    }
)


@dataclass
class DefillamaProtocols:
    """Metadata and data for all protocols."""

    metadata_df: pl.DataFrame
    app_tvl_df: pl.DataFrame
    app_token_tvl_df: pl.DataFrame


@dataclass
class SingleProtocolRecords:
    """Records obtained for a single protocol."""

    tvl_df: pl.DataFrame
    token_tvl_df: pl.DataFrame


def pull_protocol_tvl(pull_protocols: list[str] | None = None) -> DefillamaProtocols:
    """
    Pulls and processes protocol data from DeFiLlama.

    Args:
        pull_protocols: list of protocol slugs to process. Defaults to None (process all).
    """
    session = new_session()

    # Fetch the list of protocols and their metadata
    protocols = get_data(session, PROTOCOLS_ENDPOINT)
    metadata_df = extract_protocol_metadata(protocols)

    # Create list of slugs to fetch protocol-specific data
    slugs = construct_slugs(metadata_df, pull_protocols)

    # Fetch and extract protocol details in parallel.
    # The single protocol extraction filters data to only the dates of
    # interest, which helps control memory usage.
    log.info(f"fetching data for {len(slugs)} protocols")
    protocol_data: dict[str, SingleProtocolRecords] = run_concurrently(
        function=lambda slug: extract_single_protocol(session, slug),
        targets=slugs,
        max_workers=8,
    )
    log.info("done fetching and preprocessing data")

    # Load protocol data into dataframes.
    app_tvl_df = pl.concat([_.tvl_df for _ in protocol_data.values()])
    app_token_tvl_df = pl.concat([_.token_tvl_df for _ in protocol_data.values()])

    upsert_unpartitioned_table(
        df=metadata_df,
        dataset=BQ_DATASET,
        table_name=PROTOCOL_METADATA_TABLE,
        unique_keys=["protocol_slug"],
        create_if_not_exists=False,  # Set to True on first run
    )

    upsert_partitioned_table(
        df=most_recent_dates(app_tvl_df, n_dates=TVL_TABLE_LAST_N_DAYS, date_column="dt"),
        dataset=BQ_DATASET,
        table_name=PROTOCOL_TVL_DATA_TABLE,
        unique_keys=["dt", "protocol_slug", "chain"],
        create_if_not_exists=False,  # Set to True on first run
    )

    upsert_partitioned_table(
        df=most_recent_dates(app_token_tvl_df, n_dates=TVL_TABLE_LAST_N_DAYS, date_column="dt"),
        dataset=BQ_DATASET,
        table_name=PROTOCOL_TOKEN_TVL_DATA_TABLE,
        unique_keys=["dt", "protocol_slug", "chain", "token"],
        create_if_not_exists=False,  # Set to True on first run
    )

    return DefillamaProtocols(
        metadata_df=metadata_df,
        app_tvl_df=app_tvl_df,
        app_token_tvl_df=app_token_tvl_df,
    )


def extract_parent(protocol: dict[str, Any]) -> str | None:
    if parent_protcol := protocol.get("parentProtocol"):
        assert isinstance(parent_protcol, str)
        return parent_protcol.replace("parent#", "")
    else:
        return protocol.get("slug")


def extract_protocol_metadata(protocols: list[dict[str, Any]]) -> pl.DataFrame:
    """
    Extracts metadata from the protocols API response.

    Args:
        protocols: List of protocol dictionaries from the API response.

    Returns:
        Polars DataFrame containing metadata.
    """
    metadata_records = [
        {
            "protocol_name": protocol.get("name"),
            "protocol_slug": protocol.get("slug"),
            "protocol_category": protocol["category"],
            "parent_protocol": extract_parent(protocol),
        }
        for protocol in protocols
        if protocol.get("category")
    ]
    return pl.DataFrame(metadata_records)


def construct_slugs(metadata_df: pl.DataFrame, pull_protocols: list[str] | None) -> list[str]:
    """Build the collection of slugs for fetching protocol details.

    Args:
        metadata_df: DataFrame containing protocol metadata.
        pull_protocols: List of protocol slugs to process. Defaults to None (process all).
    """
    if pull_protocols is None:
        return metadata_df.get_column("protocol_slug").to_list()
    else:
        return [
            slug
            for slug in metadata_df.get_column("protocol_slug").to_list()
            if slug in pull_protocols
        ]


def extract_single_protocol(session, slug) -> SingleProtocolRecords:
    """Fetch and extract for a single protocol.

    Calls the DefiLLama endpoint and extracts daily 'tvl' and 'tokensInUsd' for the
    given slug.
    """

    # Fetch data
    url = PROTOCOL_DETAILS_ENDPOINT.format(slug=slug)
    data = get_data(session, url, retry_attempts=3)

    # Initialize extracted records.
    tvl_records = []
    token_tvl_records = []
    token_usd_tvl_records = []

    # Each app entry can have tvl data in multiple chains. Loop through each chain
    chain_tvls = data.get("chainTvls", {})
    for chain, chain_data in chain_tvls.items():
        # Extract total app tvl
        tvl_entries = chain_data.get("tvl", [])
        for tvl_entry in tvl_entries:
            dateval = dt_fromepoch(tvl_entry["date"])

            if date_fromstr(dateval) < TVL_TABLE_CUTOFF_DATE:
                continue

            if not epoch_is_date(tvl_entry["date"]):
                continue

            tvl_records.append(
                {
                    "protocol_slug": slug,
                    "chain": chain,
                    "dt": dateval,
                    "total_app_tvl": float(tvl_entry.get("totalLiquidityUSD")),
                }
            )

        # Extract token tvl for each app
        tokens_entries = chain_data.get("tokens", [])
        for tokens_entry in tokens_entries:
            dateval = dt_fromepoch(tokens_entry["date"])

            if date_fromstr(dateval) < TVL_TABLE_CUTOFF_DATE:
                continue

            if not epoch_is_date(tokens_entry["date"]):
                continue

            token_tvls = tokens_entry.get("tokens", [])
            for token in token_tvls:
                token_tvl_records.append(
                    {
                        "protocol_slug": slug,
                        "chain": chain,
                        "dt": dateval,
                        "token": token,
                        "app_token_tvl": float(token_tvls[token]),
                    }
                )

        # Extract token usd tvl for each app
        tokens_usd_entries = chain_data.get("tokensInUsd", [])
        for tokens_usd_entry in tokens_usd_entries:
            dateval = dt_fromepoch(tokens_usd_entry["date"])

            if date_fromstr(dateval) < TVL_TABLE_CUTOFF_DATE:
                continue

            if not epoch_is_date(tokens_usd_entry["date"]):
                continue

            token_usd_tvls = tokens_usd_entry.get("tokens", [])
            for token in token_usd_tvls:
                token_usd_tvl_records.append(
                    {
                        "protocol_slug": slug,
                        "chain": chain,
                        "dt": dateval,
                        "token": token,
                        "app_token_tvl_usd": float(token_usd_tvls[token]),
                    }
                )

        tvl_df = pl.DataFrame(tvl_records) if len(tvl_records) > 0 else DUMMY_TVL_DF
        token_tvl_df = (
            pl.DataFrame(token_tvl_records) if len(token_tvl_records) > 0 else DUMMY_TOKEN_TVL_DF
        )
        token_usd_tvl_df = (
            pl.DataFrame(token_usd_tvl_records)
            if len(token_usd_tvl_records) > 0
            else DUMMY_TOKEN_USD_TVL_DF
        )

        combined_df = token_tvl_df.join(
            token_usd_tvl_df,
            on=["protocol_slug", "chain", "dt", "token"],
            how="full",  # Use "outer" so we don't lose data in situations where tvl and tvl usd differ
            coalesce=True,
        )

    return SingleProtocolRecords(
        tvl_df=tvl_df,
        token_tvl_df=combined_df,
    )
