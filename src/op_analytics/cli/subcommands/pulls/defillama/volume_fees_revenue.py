import urllib.parse
from dataclasses import dataclass
from typing import Literal

import polars as pl

from op_analytics.coreutils.bigquery.write import (
    most_recent_dates,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt

from .dataaccess import DefiLlama
from .dexs.protocols import get_protocols_df
from .dexs.by_chain import get_chain_df, get_chain_breakdown_df

log = structlog.get_logger()


DEX_ENDPOINT = Literal["dailyVolume", "dailyFees", "dailyRevenue"]

ENDPOINT_MAP = {
    "dailyVolume": "dexs",
    "dailyFees": "fees",
    "dailyRevenue": "fees",
}


def summary_endpoint(data_type: DEX_ENDPOINT):
    assert data_type in ("dailyVolume", "dailyFees", "dailyRevenue")
    endpoint = ENDPOINT_MAP[data_type]
    return f"https://api.llama.fi/overview/{endpoint}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType={data_type}"


def chain_endpoint(chain: str, data_type: DEX_ENDPOINT):
    quoted_chain = urllib.parse.quote(chain)
    endpoint = ENDPOINT_MAP[data_type]
    return f"https://api.llama.fi/overview/{endpoint}/{quoted_chain}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=false&dataType={data_type}"


TABLE_LAST_N_DAYS = 3  # upsert only the last X days of volume fetched from the api


@dataclass
class DefillamaDEXData:
    """DEX dataframes at various levels of granularity."""

    # Metadata for each protocol and the daily breakdown of the protocol across all chains.
    protocols_df: pl.DataFrame

    # Total daily value of all protocols for each chain.
    chain_df: pl.DataFrame

    # Breakdown of daily value by chain and protocol.
    breakdown_df: pl.DataFrame


def execute_pull(current_dt: str | None = None):
    """DefiLlama DEX data pull.

    Pulls DEX Volume, Fees, and Revenue and writes them out to GCS.
    """

    volume: DefillamaDEXData = pull_dexs("dailyVolume")
    fees: DefillamaDEXData = pull_dexs("dailyFees")
    revenue: DefillamaDEXData = pull_dexs("dailyRevenue")

    # Join together volume fees and revenue
    chain_df = join_all(
        volume,
        fees,
        revenue,
        attr="chain_df",
        join_keys=["dt", "chain"],
    )

    breakdown_df = join_all(
        volume,
        fees,
        revenue,
        attr="breakdown_df",
        join_keys=["dt", "chain", "breakdown_name"],
    )

    return write(
        chain_df=chain_df,
        breakdown_df=breakdown_df,
        dexs_protocols_metadata_df=volume.protocols_df,
        fees_protocols_metadata_df=fees.protocols_df,
        revenue_protocols_metadata_df=revenue.protocols_df,
        current_dt=current_dt,
    )


def write_to_clickhouse():
    # Capture summaries and return them to have info in Dagster
    summaries = [
        DefiLlama.VOLUME_FEES_REVENUE.insert_to_clickhouse(incremental_overlap=3),
        DefiLlama.VOLUME_FEES_REVENUE_BREAKDOWN.insert_to_clickhouse(incremental_overlap=3),
        DefiLlama.VOLUME_PROTOCOLS_METADATA.insert_to_clickhouse(),
        DefiLlama.FEES_PROTOCOLS_METADATA.insert_to_clickhouse(),
        DefiLlama.REVENUE_PROTOCOLS_METADATA.insert_to_clickhouse(),
    ]

    return summaries


def write(
    chain_df: pl.DataFrame,
    breakdown_df: pl.DataFrame,
    dexs_protocols_metadata_df: pl.DataFrame,
    fees_protocols_metadata_df: pl.DataFrame,
    revenue_protocols_metadata_df: pl.DataFrame,
    current_dt: str | None = None,
):
    # Write by chain.
    chain_df_truncated = most_recent_dates(chain_df, n_dates=TABLE_LAST_N_DAYS)
    breakdown_df_truncated = most_recent_dates(breakdown_df, n_dates=TABLE_LAST_N_DAYS)

    DefiLlama.VOLUME_FEES_REVENUE.write(
        dataframe=chain_df_truncated,
        sort_by=["chain"],
    )

    # Write Breakdown DEX Volume.
    DefiLlama.VOLUME_FEES_REVENUE_BREAKDOWN.write(
        dataframe=breakdown_df_truncated,
        sort_by=["chain", "breakdown_name"],
    )

    # Write Protocol Metadata
    current_dt = current_dt or now_dt()

    DefiLlama.VOLUME_PROTOCOLS_METADATA.write(
        dataframe=dexs_protocols_metadata_df.with_columns(dt=pl.lit(current_dt)),
        sort_by=["defillamaId", "name"],
    )

    DefiLlama.FEES_PROTOCOLS_METADATA.write(
        dataframe=fees_protocols_metadata_df.with_columns(dt=pl.lit(current_dt)),
        sort_by=["defillamaId", "name"],
    )

    DefiLlama.REVENUE_PROTOCOLS_METADATA.write(
        dataframe=revenue_protocols_metadata_df.with_columns(dt=pl.lit(current_dt)),
        sort_by=["defillamaId", "name"],
    )

    # This return value is used to provide more information on dagster runs.
    return {
        "chain_df": len(chain_df),
        "chain_df_truncated": len(chain_df_truncated),
        "breakdown_df": len(breakdown_df),
        "breakdown_df_truncated": len(breakdown_df_truncated),
        "dexs_protocols_metadata_df": len(dexs_protocols_metadata_df),
        "fees_protocols_metadata_df": len(fees_protocols_metadata_df),
        "revenue_protocols_metadata_df": len(revenue_protocols_metadata_df),
    }


def pull_dexs(data_type: DEX_ENDPOINT) -> DefillamaDEXData:
    """Pull all DEX dataframes for a given data type.

    The DefiLlama dexs API provides 3 data types:
    - Volume
    - Fees
    - Revenue

    For each data type we collect protocol, all of crypto, by chain, and by chain and protocol.
    """

    session = new_session()

    summary_response = get_data(session, summary_endpoint(data_type))

    # Summary Data
    protocols_df = get_protocols_df(summary_response)

    # Chain and Protocol Level Data
    chain_responses = get_chain_responses(session, summary_response, data_type)
    chain_df = get_chain_df(chain_responses)
    breakdown_df = get_chain_breakdown_df(chain_responses)

    return DefillamaDEXData(
        protocols_df=protocols_df,
        chain_df=chain_df,
        breakdown_df=breakdown_df,
    )


def get_chain_responses(session, summary_response, data_type: DEX_ENDPOINT) -> dict[str, dict]:
    chain_urls = {}
    for chain in summary_response["allChains"]:
        chain_urls[chain] = chain_endpoint(chain, data_type)
    chain_responses = run_concurrently(lambda x: get_data(session, x), chain_urls, max_workers=4)
    return chain_responses


def join_all(
    volume: DefillamaDEXData,
    fees: DefillamaDEXData,
    revenue: DefillamaDEXData,
    attr: str,
    join_keys: list[str],
) -> pl.DataFrame:
    """Helper function to join volume, fees, and revenue dataframes.

    At each level of granularity the join is similar, this helper function takes care of the
    boilerplate.
    """

    assert attr in ("chain_df", "breakdown_df")

    volume_df: pl.DataFrame = getattr(volume, attr).rename({"total_usd": "total_volume_usd"})
    fees_df: pl.DataFrame = getattr(fees, attr).rename({"total_usd": "total_fees_usd"})
    revenue_df: pl.DataFrame = getattr(revenue, attr).rename({"total_usd": "total_revenue_usd"})

    return volume_df.join(
        fees_df,
        on=join_keys,
        how="full",
        coalesce=True,
        validate="1:1",
    ).join(
        revenue_df,
        on=join_keys,
        how="full",
        coalesce=True,
        validate="1:1",
    )
