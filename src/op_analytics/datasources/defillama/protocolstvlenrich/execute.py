from datetime import date, timedelta

import polars as pl


from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import memory_usage, structlog, bound_contextvars
from op_analytics.coreutils.partitioned.dailydata import ParquetPathNotFound
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr, now_date
from op_analytics.coreutils.misc import raise_for_schema_mismatch

from ..dataaccess import DefiLlama
from .calculate import calculate_net_flows
from .read import read_compute_at_data, read_lookback_data, read_metadata
from .enrich import enrich_all

log = structlog.get_logger()


FLOW_TABLE_LAST_N_DAYS = 90


# NOTE: If this is changed then the output schema will change since we add one
# additional column to the output for each of these lookback values.
FLOW_DAYS = [1, 7, 14, 28, 60, 90, 365]


def execute_pull(range_spec: str | None = None, force_complete: bool = True):
    if range_spec is None:
        process_date = now_date()
        dates = [process_date - timedelta(days=_) for _ in range(FLOW_TABLE_LAST_N_DAYS)]

    else:
        dates = DateRange.from_spec(range_spec).dates()
        dates.reverse()

    # Get written markers.
    markers = DefiLlama.PROTOCOL_TVL_FLOWS_FILTERED.written_markers_datevals(datevals=dates)
    complete_dates = set(markers["dt"].to_list())

    # Get metadata using  a recent date.
    metadata_df = read_metadata(now_date() - timedelta(days=1))

    # Iterate over the dates to compute
    written_rows = {}
    for dateval in dates:
        datestr = date_tostr(dateval)

        with bound_contextvars(compute_date=datestr):
            # Check if we should skip this date.
            if dateval in complete_dates:
                if force_complete:
                    log.info("force complete")
                else:
                    log.info("net flows already complete")
                    continue

            # Read data and compute the net flows at "compute_date".
            try:
                result = run(metadata_df=metadata_df, compute_date=dateval)
            except ParquetPathNotFound:
                log.warning("no PROTOCOL_TOKEN_TVL data found")
                continue

            # Write out the result
            DefiLlama.PROTOCOL_TVL_FLOWS_FILTERED.write(
                dataframe=result,
                sort_by=["dt", "chain", "protocol_slug", "token"],
            )
            written_rows[datestr] = len(result)

            # Log memory use and clean up duckdb views.
            log.info("memory usage", max_rss=memory_usage())
            ctx = init_client()
            ctx.unregister_views()

    return written_rows


def run(metadata_df: pl.DataFrame, compute_date: date | None = None):
    compute_date = compute_date or now_date()
    log.info(f"computing net flows at {compute_date=}")

    # Read data.
    compute_date_df = read_compute_at_data(compute_date)
    lookback_df = read_lookback_data(compute_date, FLOW_DAYS)

    # Net Flows
    with_flows = calculate_net_flows(
        df=compute_date_df,
        lookback_df=lookback_df,
        flow_days=FLOW_DAYS,
    )

    # Enrich
    enriched = enrich_all(
        df=with_flows,
        metadata_df=metadata_df,
    )

    raise_for_schema_mismatch(enriched.schema, EXPECTED_SCHEMA)
    return enriched


EXPECTED_SCHEMA = pl.Schema(
    {
        "dt": pl.Datetime(time_unit="us", time_zone=None),
        "chain": pl.String(),
        "protocol_slug": pl.String(),
        "token": pl.String(),
        "app_token_tvl": pl.Float64(),
        "app_token_tvl_usd": pl.Float64(),
        "usd_conversion_rate": pl.Float64(),
        #
        "app_token_tvl_1d": pl.Float64(),
        "app_token_tvl_7d": pl.Float64(),
        "app_token_tvl_14d": pl.Float64(),
        "app_token_tvl_28d": pl.Float64(),
        "app_token_tvl_60d": pl.Float64(),
        "app_token_tvl_90d": pl.Float64(),
        "app_token_tvl_365d": pl.Float64(),
        #
        "net_token_flow_1d": pl.Float64(),
        "net_token_flow_7d": pl.Float64(),
        "net_token_flow_14d": pl.Float64(),
        "net_token_flow_28d": pl.Float64(),
        "net_token_flow_60d": pl.Float64(),
        "net_token_flow_90d": pl.Float64(),
        "net_token_flow_365d": pl.Float64(),
        "protocol_name": pl.String(),
        "protocol_category": pl.String(),
        "parent_protocol": pl.String(),
        "misrepresented_tokens": pl.Int32(),
        "is_protocol_misrepresented": pl.Int32(),
        "is_double_counted": pl.Int8(),
        "to_filter_out": pl.Int8(),
    }
)
