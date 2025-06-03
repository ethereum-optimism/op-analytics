import polars as pl
import requests

from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.threads import run_concurrently


from .utils import apply_schema, L2BeatProject


TVL_SCHEMA: dict[str, type[pl.DataType]] = {
    "timestamp": pl.Int64,
    "native": pl.Float64,
    "canonical": pl.Float64,
    "external": pl.Float64,
    "ethPrice": pl.Float64,
}


def fetch_tvl(
    projects: list[L2BeatProject],
    query_range: str,
    session: requests.Session | None = None,
):
    session = session or new_session()

    # Call api
    all_data = run_concurrently(
        function=lambda x: get_data(
            session,
            url=f"https://l2beat.com/api/scaling/tvs/{x.slug}?range={query_range}",
            retry_attempts=5,
        ),
        targets=projects,
        max_workers=2,
    )

    # Convert to dataframe
    return apply_schema(all_data, TVL_SCHEMA)
