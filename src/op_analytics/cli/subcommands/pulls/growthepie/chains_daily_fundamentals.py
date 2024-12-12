from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session

from .dataaccess import GrowThePie

log = structlog.get_logger()

URL_BASE = "https://api.growthepie.xyz/"
FUNDAMENTALS_ENDPOINT = "v1/fundamentals_full.json"
METADATA_ENDPOINT = "v1/master.json"


@dataclass
class GrowthepieFundamentalSummary:
    """Summary of daily chain fundamentals from GrowThePie.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    metadata_df: pl.DataFrame
    summary_df: pl.DataFrame


def pull_growthepie_summary() -> GrowthepieFundamentalSummary:
    """Pull data from GrowThePie.

    - Fetch GrowThePie daily chain fundamentals summary.
    """
    session = new_session()

    summary_raw_data = get_data(session, f"{URL_BASE}{FUNDAMENTALS_ENDPOINT}")
    summary_df = pl.DataFrame(summary_raw_data)

    GrowThePie.FUNDAMENTALS_SUMMARY.write(
        dataframe=summary_df,
        sort_by=["origin_key", "date"],
    )

    metadata_raw_data = get_data(session, f"{URL_BASE}{METADATA_ENDPOINT}")
    chains_meta = metadata_raw_data["chains"]

    chains_meta_processed = process_metadata_pull(chains_meta)
    metadata_df = pl.DataFrame(chains_meta_processed)

    GrowThePie.CHAIN_METADATA.write(
        dataframe=metadata_df,
        sort_by=["name"],
    )

    return GrowthepieFundamentalSummary(
        metadata_df=metadata_df,
        summary_df=summary_df,
    )


def process_metadata_pull(df) -> list[str]:
    """
    Extracts chain metadata from GrowThePie API response.
    """
    normalized_data = []

    # Iterate over each chain in the 'chains' data
    for chain_meta, attributes in df.items():
        if isinstance(attributes, dict):
            # Add the 'chain_meta' as part of the attributes
            attributes["origin_key"] = chain_meta
            normalized_data.append(attributes)

    return normalized_data
