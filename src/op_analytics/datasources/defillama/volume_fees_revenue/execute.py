from typing import Literal

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.time import now_dt

from ..dataaccess import DefiLlama
from .helpers import DefillamaVFRData, join_all

log = structlog.get_logger()


DEX_ENDPOINT = Literal["dailyVolume", "dailyFees", "dailyRevenue"]


TABLE_LAST_N_DAYS = 3  # upsert only the last X days of volume fetched from the api


def execute_pull(current_dt: str | None = None):
    """DefiLlama DEX data pull.

    Pulls DEX Volume, Fees, and Revenue and writes them out to GCS.
    """

    volume = DefillamaVFRData.of("dailyVolume")
    fees = DefillamaVFRData.of("dailyFees")
    revenue = DefillamaVFRData.of("dailyRevenue")

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
        "chain_df": dt_summary(chain_df),
        "chain_df_truncated": dt_summary(chain_df_truncated),
        "breakdown_df": dt_summary(breakdown_df),
        "breakdown_df_truncated": dt_summary(breakdown_df_truncated),
        "dexs_protocols_metadata_df": dt_summary(dexs_protocols_metadata_df),
        "fees_protocols_metadata_df": dt_summary(fees_protocols_metadata_df),
        "revenue_protocols_metadata_df": dt_summary(revenue_protocols_metadata_df),
    }
