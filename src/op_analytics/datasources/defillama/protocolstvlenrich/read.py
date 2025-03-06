from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import date_tostr

from ..dataaccess import DefiLlama

log = structlog.get_logger()


TVL_DEDUPE_COLUMNS = ["dt", "chain", "protocol_slug", "token"]


def deduplicate_df(
    df: pl.DataFrame, dedupe_cols: list[str], force_dedupe: bool = False
) -> pl.DataFrame:
    duplicates = df.group_by(dedupe_cols).len().filter(pl.col("len") > 1)
    if len(duplicates) == 0:
        return df

    # We found duplicates. To process a date with duplicates one should manually
    # set force_dedupe = True.
    if not force_dedupe:
        raise Exception("found duplicates on dataframe")
    else:
        deduped = df.unique(subset=dedupe_cols, keep="any")
        log.warning(f"removed {len(df) - len(deduped)} duplicate rows from dataframe")
        return deduped


def read_compute_at_data(compute_date: date):
    """Reads TVL data at the date we are computing.

    NOTE: This includes adding the "usd_conversion_rate" column.
    """

    ctx = init_client()
    client = ctx.client

    log.info(f"reading token tvl data at {compute_date=}")
    at_compute = DefiLlama.PROTOCOLS_TOKEN_TVL.read_datevals(
        datevals=[compute_date],
        view_name="at_compute",
    )
    compute_date_df = client.sql(
        f"""
        SELECT
            dt,
            chain,
            protocol_slug,
            token,
            app_token_tvl,
            app_token_tvl_usd,
        FROM {at_compute}
        """
    ).pl()

    return deduplicate_df(df=compute_date_df, dedupe_cols=TVL_DEDUPE_COLUMNS)


def read_lookback_data(compute_date: date, flow_days: list[int]):
    """Reads TVL data at the lookback dates that are used for the NET FLOW calculation."""
    ctx = init_client()
    client = ctx.client

    datevals = []
    for looback in flow_days:
        datevals.append(compute_date - timedelta(days=looback))

    dtvals = [date_tostr(_) for _ in datevals]
    log.info(f"reading token tvl data for {dtvals=}")
    lookback = DefiLlama.PROTOCOLS_TOKEN_TVL.read_datevals(
        datevals=datevals,
        view_name="lookback",
    )

    # In this query we include the delta days to the compute day. This lets us
    # easily filter down to one of the specific lookback dates later on.
    looback_df = client.sql(
        f"""
        SELECT
            dt,
            DATE '{date_tostr(compute_date)}' - dt - 1 AS lookback,
            chain,
            protocol_slug,
            token,
            COALESCE(app_token_tvl, 0) as app_token_tvl
        FROM {lookback}
        """
    ).pl()

    return deduplicate_df(df=looback_df, dedupe_cols=TVL_DEDUPE_COLUMNS)


def read_metadata(dateval: date):
    """Reads TVL metadata data at the date we are computing."""

    ctx = init_client()
    client = ctx.client

    log.info(f"reading token tvl metadata at {dateval=}")
    DefiLlama.PROTOCOLS_METADATA.read_datevals(
        datevals=[dateval],
        view_name="metadata",
    )

    # Process metadata
    metadata_df = client.sql("""
        SELECT 
            protocol_name,
            protocol_slug,
            protocol_category,
            parent_protocol,
            CASE 
                WHEN misrepresented_tokens = 'True' THEN 1
                ELSE 0
            END AS misrepresented_tokens
        FROM metadata
    """).pl()

    return deduplicate_df(df=metadata_df, dedupe_cols=["protocol_slug"], force_dedupe=True)
