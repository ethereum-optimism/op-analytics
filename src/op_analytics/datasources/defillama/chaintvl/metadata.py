from dataclasses import dataclass

import polars as pl
import requests

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data

log = structlog.get_logger()

CHAINS_METADATA_ENDPOINT = "https://api.llama.fi/config"
CHAINS_ENDPOINT = "https://api.llama.fi/v2/chains"
CHAINS_TVL_ENDPOINT = "https://api.llama.fi/v2/historicalChainTvl/{slug}"


TVL_TABLE_LAST_N_DAYS = 90


@dataclass
class ChainsMetadata:
    df: pl.DataFrame
    chains: list[str]

    @classmethod
    def fetch(cls, session: requests.Session) -> "ChainsMetadata":
        # Call the chains endpoint to find the list of chains tracked by DefiLLama.
        chains_data = get_data(session, CHAINS_ENDPOINT)
        dfl_chains_list = get_dfl_chains(chains_data)

        # Call the config endpoint to find metadata for chains.
        metadata = get_data(session, CHAINS_METADATA_ENDPOINT)
        metadata_df = extract_chain_metadata(metadata["chainCoingeckoIds"], dfl_chains_list)

        return cls(df=metadata_df, chains=dfl_chains_list)


def get_dfl_chains(summary) -> list[str]:
    """
    Extracts chain names from defillama chains end point
    """
    return [chain.get("name") for chain in summary]


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
