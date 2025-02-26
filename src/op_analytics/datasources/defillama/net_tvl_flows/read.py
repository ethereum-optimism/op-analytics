from datetime import date, timedelta


from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import date_tostr

from ..dataaccess import DefiLlama

log = structlog.get_logger()


def read_compute_at_data(compute_date: date):
    """Reads TVL data at the date we are computing.

    NOTE: This includes adding the "usd_conversion_rate" column.
    """

    ctx = init_client()
    client = ctx.client

    log.info(f"reading token tvl data at {compute_date=}")
    at_compute = DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.read_datevals(
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
            COALESCE(app_token_tvl, 0) AS app_token_tvl,
            COALESCE(app_token_tvl_usd, 0) AS app_token_tvl_usd,
            app_token_tvl_usd / app_token_tvl AS usd_conversion_rate
        FROM {at_compute}
        """
    ).pl()
    return compute_date_df


def read_lookback_data(compute_date: date, flow_days: list[int]):
    """Reads TVL data at the lookback dates that are used for the NET FLOW calculation."""
    ctx = init_client()
    client = ctx.client

    datevals = []
    for looback in flow_days:
        datevals.append(compute_date - timedelta(days=looback))

    log.info(f"reading token tvl data for {datevals=}")
    lookback = DefiLlama.PROTOCOL_TOKEN_TVL_BREAKDOWN.read_datevals(
        datevals=datevals,
        view_name="lookback",
    )

    # In this query we include the delta days to the compute day. This lets us
    # easily filter down to one of the specific lookback dates later on.
    looback_df = client.sql(
        f"""
        SELECT
            DATE '{date_tostr(compute_date)}' - dt AS lookback,
            chain,
            protocol_slug,
            token,
            COALESCE(app_token_tvl, 0) as app_token_tvl
        FROM {lookback}
        """
    ).pl()

    return looback_df
