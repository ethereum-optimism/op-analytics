from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import memory_usage, structlog
from op_analytics.coreutils.partitioned.dailydata import ParquetPathNotFound
from op_analytics.coreutils.time import date_tostr, now_date

from ..dataaccess import DefiLlama
from .calculate import calculate_net_flows
from .read import read_compute_at_data, read_lookback_data

log = structlog.get_logger()


FLOW_TABLE_LAST_N_DAYS = 720


# NOTE: If this is changed then the output schema will change since we add one
# additional column to the output for each of these lookback values.
FLOW_DAYS = [1, 7, 14, 28, 60, 90, 365]


def execute_pull(process_date: date | None = None):
    process_date = process_date or now_date()
    written_rows = {}

    dates_to_compute = [process_date - timedelta(days=_) for _ in range(FLOW_TABLE_LAST_N_DAYS)]

    for compute_date in dates_to_compute:
        # Read data and compute the net flows at "compute_date".
        try:
            result = DefiLlamaNetFlows.at_date(compute_date=compute_date)
        except ParquetPathNotFound:
            log.warning(f"no PROTOCOL_TOKEN_TVL_BREAKDOWN data found at {compute_date=}")
            continue

        # Write out the result
        DefiLlama.PROTOCOL_TOKEN_NET_TVL_FLOWS.write(
            dataframe=result.df,
            sort_by=["dt", "chain", "protocol_slug", "token"],
        )
        written_rows[date_tostr(compute_date)] = len(result.df)

        # Log memory use and clean up duckdb views.
        log.info("memory usage", max_rss=memory_usage())
        ctx = init_client()
        ctx.unregister_views()

    return written_rows


@dataclass
class DefiLlamaNetFlows:
    df: pl.DataFrame

    @classmethod
    def at_date(cls, compute_date: date | None = None):
        """Process DeFiLlama TVL data to calculate net flows."""
        compute_date = compute_date or now_date()

        log.info(f"computing net flows at {compute_date=}")

        # Read data.
        compute_date_df = read_compute_at_data(compute_date)
        lookback_df = read_lookback_data(compute_date, FLOW_DAYS)

        # Calculate
        df_flows = calculate_net_flows(
            df=compute_date_df,
            lookback_df=lookback_df,
            flow_days=FLOW_DAYS,
        )
        return cls(df=df_flows)
