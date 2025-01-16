from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import dt_fromepoch, now_dt

from .dataaccess import DefiLlama

log = structlog.get_logger()

CHAINS_METADATA_ENDPOINT = "https://api.llama.fi/config"
CHAINS_ENDPOINT = "https://api.llama.fi/v2/chains"
CHAINS_TVL_ENDPOINT = "https://api.llama.fi/v2/historicalChainTvl/{slug}"


TVL_TABLE_LAST_N_DAYS = 7


@dataclass
class DefillamaChains:
    """Metadata and tvl for all chains.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    metadata_df: pl.DataFrame
    tvl_df: pl.DataFrame


def pull_historical_chain_tvl(pull_chains: list[str] | None = None) -> DefillamaChains:
    """
    Pulls and processes historical chain TVL data from DeFiLlama.

    Args:
        pull_chains: list of chain names (slugs) to process. Defaults to None (process all).
    """
    session = new_session()

    # Call the chains endpoint to find the list of chains tracked by DefiLLama.
    chains_data = get_data(session, CHAINS_ENDPOINT)
    dfl_chains_list = get_dfl_chains(chains_data)

    # Call the config endpoint to find metadata for chains.
    metadata = get_data(session, CHAINS_METADATA_ENDPOINT)
    metadata_df = extract_chain_metadata(metadata["chainCoingeckoIds"], dfl_chains_list)

    DefiLlama.CHAINS_METADATA.write(
        dataframe=metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["chain_name"],
    )

    # Call the API endpoint for each stablecoin in parallel.
    urls = construct_urls(dfl_chains_list, pull_chains, CHAINS_TVL_ENDPOINT)
    historical_chain_tvl_data = run_concurrently(
        lambda x: get_data(session, x), urls, max_workers=4
    )

    # Extract daily tvl from historical chains tvl data.
    tvl_df = extract_chain_tvl_to_dataframe(historical_chain_tvl_data)

    # Write balances.
    DefiLlama.HISTORICAL_CHAIN_TVL.write(
        dataframe=most_recent_dates(tvl_df, n_dates=TVL_TABLE_LAST_N_DAYS, date_column="dt"),
        sort_by=["chain_name"],
    )

    return DefillamaChains(
        metadata_df=metadata_df,
        tvl_df=tvl_df,
    )


def execute_pull():
    result = pull_historical_chain_tvl()
    return {
        "metadata_df": len(result.metadata_df),
        "tvl_df": len(result.tvl_df),
    }


def extract_category_data(row: dict, category_type: str) -> int:
    """Checks to see if chain metadata contains an EVM category"""
    return 1 if "categories" in row and category_type in row["categories"] else 0


def extract_layer(row: dict) -> str:
    """Extracts L2 and L3 information from metadata. Defaults to L1."""
    parent = row.get("parent")
    parent_types = parent.get("types") if parent else []

    if "L2" in parent_types:
        return "L2"
    elif "L3" in parent_types:
        return "L3"
    else:
        return "L1"


def extract_chain_metadata(chain_metadata: dict, dfl_chains: list) -> pl.DataFrame:
    """
    Extracts metadata from the config end point.
    Data includes extra chains that Defillama does not track TVL for.
    Entries may be null for certain chains.

    Args:
        chain_metadata: the "chainCoingeckoIds" response from config API
        df: the summary df from the chains end point used to determine if
            defillama tracks TVL for a particular chain

    """
    chain_metadata_records = []

    for chain in chain_metadata:
        chain_data = chain_metadata[chain]

        chain_id = chain_data.get("chainId")
        gecko_id = chain_data.get("geckoId")
        cmc_id = chain_data.get("cmcId")
        symbol = chain_data.get("symbol")
        is_evm = extract_category_data(chain_data, "EVM")
        is_superchain = extract_category_data(chain_data, "Superchain")
        layer = extract_layer(chain_data)

        chain_metadata_records.append(
            {
                "chain_name": chain,
                "chain_id": chain_id,
                "dfl_tracks_tvl": 1 if chain in dfl_chains else 0,
                "is_evm": is_evm,
                "is_superchain": is_superchain,
                "layer": layer,
                "is_rollup": 0 if layer == "L1" else 1,
                "gecko_id": gecko_id,
                "cmc_id": cmc_id,
                "symbol": symbol,
            }
        )

    return pl.DataFrame(chain_metadata_records)


def get_dfl_chains(summary) -> list[str]:
    """
    Extracts chain names from defillama chains end point
    """
    return [chain.get("name") for chain in summary]


def construct_urls(
    chain_list: list[str], pull_chains: list[str] | None, endpoint: str
) -> dict[str, str]:
    """Build the collection of urls that we will fetch from DefiLlama.

    Args:
        pull_chains: list of chains to process. Defaults to None (process all).
        endpoint: the API end point needed to pull historical chain TVL
    """

    if pull_chains is None:
        slugs = chain_list
    else:
        slugs = [chain for chain in chain_list if chain in pull_chains]

    urls = {slug: endpoint.format(slug=slug) for slug in slugs}

    if not urls:
        raise ValueError("No valid slugs provided.")
    return urls


def extract_chain_tvl_to_dataframe(data) -> pl.DataFrame:
    """Extract historical TVL and transform into dataframe"""
    records = [
        {"chain_name": chain, "dt": dt_fromepoch(entry["date"]), "tvl": entry["tvl"]}
        for chain, entries in data.items()
        for entry in entries
    ]

    return pl.DataFrame(records)
