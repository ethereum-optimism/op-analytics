from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary, last_n_days
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import now_dt

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


def execute_pull():
    result = pull_growthepie_summary()
    return {
        "metadata_df": dt_summary(result.metadata_df),
        "summary_df": dt_summary(result.summary_df),
    }


def pull_growthepie_summary() -> GrowthepieFundamentalSummary:
    """Pull data from GrowThePie.

    - Fetch GrowThePie daily chain fundamentals summary.
    """
    session = new_session()
    current_dt: str = now_dt()

    summary_raw_data = get_data(session, f"{URL_BASE}{FUNDAMENTALS_ENDPOINT}")
    summary_df = pl.DataFrame(summary_raw_data)
    summary_df_truncated = last_n_days(summary_df, n_dates=7, reference_dt=current_dt)

    summary_df = summary_df.rename({"date": "dt"})

    GrowThePie.FUNDAMENTALS_SUMMARY.write(
        # Use the full dataframe when backfilling:
        # dataframe=summary_df,
        dataframe=summary_df_truncated,
        sort_by=["origin_key"],
    )

    metadata_raw_data = get_data(session, f"{URL_BASE}{METADATA_ENDPOINT}")
    chains_meta = metadata_raw_data["chains"]

    chains_meta_processed = process_metadata_pull(chains_meta)
    metadata_df = pl.DataFrame(chains_meta_processed)

    GrowThePie.CHAIN_METADATA.write(
        dataframe=metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["name"],
    )

    return GrowthepieFundamentalSummary(
        metadata_df=metadata_df,
        summary_df=summary_df_truncated,
    )


def process_metadata_pull(df) -> list[dict[str, str]]:
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
