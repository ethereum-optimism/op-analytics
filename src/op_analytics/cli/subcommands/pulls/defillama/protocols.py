from dataclasses import dataclass
import polars as pl
from op_analytics.coreutils.bigquery.write import (
    most_recent_dates,
    upsert_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import dt_fromepoch

log = structlog.get_logger()

PROTOCOLS_ENDPOINT = "https://api.llama.fi/protocols"
PROTOCOL_DETAILS_ENDPOINT = "https://api.llama.fi/protocol/{slug}"

BQ_DATASET = "uploads_api"
PROTOCOL_METADATA_TABLE = "defillama_protocols_metadata"
PROTOCOL_TVL_DATA_TABLE = "defillama_protocols_tvl"
PROTOCOL_TOKEN_TVL_DATA_TABLE = "defillama_protocols_token_tvl"

TVL_TABLE_LAST_N_DAYS = 7


@dataclass
class DefillamaProtocols:
    """Metadata and data for all protocols."""

    metadata_df: pl.DataFrame
    app_tvl_df: pl.DataFrame
    app_token_tvl_df: pl.DataFrame


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

    # Construct URLs for protocol-specific data
    urls = construct_urls(metadata_df, pull_protocols, PROTOCOL_DETAILS_ENDPOINT)
    # Fetch protocol details in parallel
    protocol_data = run_concurrently(lambda url: get_data(session, url), urls, max_workers=4)

    # Extract protocol data into a dataframe
    app_tvl_df, app_token_tvl_df = extract_protocol_tvl_to_dataframes(protocol_data)

    upsert_unpartitioned_table(
        df=metadata_df,
        dataset=BQ_DATASET,
        table_name=PROTOCOL_METADATA_TABLE,
        unique_keys=["protocol_slug"],
        create_if_not_exists=False,  # Set to True on first run
    )

    upsert_unpartitioned_table(
        df=most_recent_dates(app_tvl_df, n_dates=TVL_TABLE_LAST_N_DAYS, date_column="date"),
        dataset=BQ_DATASET,
        table_name=PROTOCOL_TVL_DATA_TABLE,
        unique_keys=["protocol_slug", "chain", "date"],
        create_if_not_exists=False,  # Set to True on first run
    )

    upsert_unpartitioned_table(
        df=most_recent_dates(app_token_tvl_df, n_dates=TVL_TABLE_LAST_N_DAYS, date_column="date"),
        dataset=BQ_DATASET,
        table_name=PROTOCOL_TOKEN_TVL_DATA_TABLE,
        unique_keys=["protocol_slug", "chain", "date", "token"],
        create_if_not_exists=False,  # Set to True on first run
    )

    return DefillamaProtocols(
        metadata_df=metadata_df,
        app_tvl_df=app_tvl_df,
        app_token_tvl_df=app_token_tvl_df,
    )


def extract_protocol_metadata(protocols: list[dict]) -> pl.DataFrame:
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
            "protocol_category": protocol.get("category"),
            "parent_protocol": protocol.get("parentProtocol").replace("parent#", "")
            if protocol.get("parentProtocol")
            else protocol.get("slug"),
        }
        for protocol in protocols
    ]
    return pl.DataFrame(metadata_records)


def construct_urls(
    metadata_df: pl.DataFrame, pull_protocols: list[str] | None, endpoint: str
) -> dict[str, str]:
    """Build the collection of URLs for fetching protocol details.

    Args:
        metadata_df: DataFrame containing protocol metadata.
        pull_protocols: List of protocol slugs to process. Defaults to None (process all).
        endpoint: The API endpoint for protocol details.

    Returns:
        A dictionary of protocol slugs and their corresponding URLs.
    """
    if pull_protocols is None:
        slugs = metadata_df.get_column("protocol_slug").to_list()
    else:
        slugs = [
            slug
            for slug in metadata_df.get_column("protocol_slug").to_list()
            if slug in pull_protocols
        ]

    urls = {slug: endpoint.format(slug=slug) for slug in slugs}
    if not urls:
        raise ValueError("No valid slugs provided.")
    return urls


def extract_protocol_tvl_to_dataframes(protocol_data: dict) -> pl.DataFrame:
    """
    Extracts daily 'tvl' and 'tokensInUsd' data for each protocol across chains.

    Args:
        protocol_data: A dictionary where each key is a protocol slug, and the value contains protocol details.

    Returns:
        Two polars dataframes containing app tvl data and app token tvl data
    """
    app_tvl_records = []
    app_token_tvl_records = []

    # Loop through each entry in protocol_data
    for slug, details in protocol_data.items():
        chain_tvls = details.get("chainTvls", {})

        # Each app entry can have tvl data in multiple chains. Loop through each chain
        for chain, chain_data in chain_tvls.items():
            # Extract total app tvl
            tvl_entries = chain_data.get("tvl", [])
            for tvl_entry in tvl_entries:
                app_tvl_records.append(
                    {
                        "protocol_slug": slug,
                        "chain": chain,
                        "date": dt_fromepoch(tvl_entry["date"]),
                        "total_app_tvl": tvl_entry.get("totalLiquidityUSD"),
                    }
                )

            # Extract token tvl for each app
            tokens_entries = chain_data.get("tokensInUsd", [])
            for tokens_entry in tokens_entries:
                date = dt_fromepoch(tokens_entry["date"])
                token_tvls = tokens_entry.get("tokens", [])
                for token in token_tvls:
                    app_token_tvl_records.append(
                        {
                            "protocol_slug": slug,
                            "chain": chain,
                            "date": date,
                            "token": token,
                            "app_token_tvl": token_tvls[token],
                        }
                    )

    return pl.DataFrame(app_tvl_records), pl.DataFrame(app_token_tvl_records)
