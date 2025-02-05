import urllib.parse
from dataclasses import dataclass
from typing import Literal

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently

from .by_chain import get_chain_breakdown_df, get_chain_df
from .protocols import get_protocols_df

log = structlog.get_logger()


ENDPOINT_TYPES = Literal["dailyVolume", "dailyFees", "dailyRevenue"]

ENDPOINT_MAP = {
    "dailyVolume": "dexs",
    "dailyFees": "fees",
    "dailyRevenue": "fees",
}


def summary_endpoint(data_type: ENDPOINT_TYPES):
    assert data_type in ("dailyVolume", "dailyFees", "dailyRevenue")
    endpoint = ENDPOINT_MAP[data_type]
    return f"https://api.llama.fi/overview/{endpoint}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType={data_type}"


def chain_endpoint(chain: str, data_type: ENDPOINT_TYPES):
    quoted_chain = urllib.parse.quote(chain)
    endpoint = ENDPOINT_MAP[data_type]
    return f"https://api.llama.fi/overview/{endpoint}/{quoted_chain}?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=false&dataType={data_type}"


@dataclass
class DefillamaVFRData:
    """VFR = Volume, fees, revenue.

    Includes the "protocols" metadata dataframe and chain + protocol breakdowns.

    Keep in mind that "protocol" can mean many different things in DefiLlama.
    """

    # Metadata for each protocol and the daily breakdown of the protocol across all chains.
    protocols_df: pl.DataFrame

    # Total daily value of all protocols for each chain.
    chain_df: pl.DataFrame

    # Breakdown of daily value by chain and protocol.
    breakdown_df: pl.DataFrame

    @classmethod
    def of(cls, data_type: ENDPOINT_TYPES) -> "DefillamaVFRData":
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

        return cls(
            protocols_df=protocols_df,
            chain_df=chain_df,
            breakdown_df=breakdown_df,
        )


def get_chain_responses(session, summary_response, data_type: ENDPOINT_TYPES) -> dict[str, dict]:
    chain_urls = {}
    for chain in summary_response["allChains"]:
        chain_urls[chain] = chain_endpoint(chain, data_type)
    chain_responses = run_concurrently(lambda x: get_data(session, x), chain_urls, max_workers=4)
    return chain_responses


def join_all(
    volume: DefillamaVFRData,
    fees: DefillamaVFRData,
    revenue: DefillamaVFRData,
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
