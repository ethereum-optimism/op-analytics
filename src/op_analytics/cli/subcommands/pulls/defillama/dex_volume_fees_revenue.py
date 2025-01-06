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
from .dexs.total_data_chart import get_total_data_chart_df
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


TABLE_LAST_N_DAYS = 4000  # upsert only the last X days of volume fetched from the api


@dataclass
class DefillamaDEXData:
    """DEX dataframes at various levels of granularity."""

    # Metadata for each protocol and the daily breakdown of the protocol across all chains.
    protocols_df: pl.DataFrame

    # Total daily value across all crypto (chains and protocols)
    crypto_df: pl.DataFrame

    # Total daily value of all protocols for each chain.
    chain_df: pl.DataFrame

    # Breakdown of daily value by chain and protocol.
    chain_protocol_df: pl.DataFrame


def pull_dex_dataframes(current_dt: str | None = None):
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

    chain_protocol_df = join_all(
        volume,
        fees,
        revenue,
        attr="chain_protocol_df",
        join_keys=["dt", "chain", "protocol"],
    )

    crypto_df = join_all(
        volume,
        fees,
        revenue,
        attr="crypto_df",
        join_keys=["dt"],
    )

    write(
        crypto_df=crypto_df,
        chain_df=chain_df,
        chain_protocol_df=chain_protocol_df,
        # Assume the protocols metadata is the same as returned by dexs/ or fees/ endpoints.
        protocols_metadata_df=volume.protocols_df,
        current_dt=current_dt,
    )


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
    crypto_df = get_total_data_chart_df(summary_response)
    protocols_df = get_protocols_df(summary_response)

    # Chain and Protocol Level Data
    chain_responses = get_chain_responses(session, summary_response, data_type)
    chain_df = get_chain_df(chain_responses)
    chain_protocol_df = get_chain_breakdown_df(chain_responses)

    return DefillamaDEXData(
        crypto_df=crypto_df,
        protocols_df=protocols_df,
        chain_df=chain_df,
        chain_protocol_df=chain_protocol_df,
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

    assert attr in ("crypto_df", "chain_df", "chain_protocol_df")

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


def write(
    crypto_df: pl.DataFrame,
    chain_df: pl.DataFrame,
    chain_protocol_df: pl.DataFrame,
    protocols_metadata_df: pl.DataFrame,
    current_dt: str | None = None,
):
    # Write Overall DEX Volume.
    DefiLlama.DEXS_FEES_TOTAL.write(
        dataframe=most_recent_dates(crypto_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["dt"],
    )

    # Write By Chain DEX Volume.
    DefiLlama.DEXS_FEES_BY_CHAIN.write(
        dataframe=most_recent_dates(chain_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain"],
    )

    # Write Breakdown DEX Volume.
    DefiLlama.DEXS_FEES_BY_CHAIN_PROTOCOL.write(
        dataframe=most_recent_dates(chain_protocol_df, n_dates=TABLE_LAST_N_DAYS),
        sort_by=["chain", "protocol"],
    )

    # Write Protocol Metadata
    current_dt = current_dt or now_dt()

    DefiLlama.DEXS_PROTOCOLS_METADATA.write(
        dataframe=protocols_metadata_df.with_columns(dt=pl.lit(current_dt)),
        sort_by=["defillamaId", "name"],
    )
