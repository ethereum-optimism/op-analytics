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


def fetch_project_data(project, session, query_range):
    result = get_data(
        session,
        url=f"https://l2beat.com/api/scaling/tvs/{project.slug}?range={query_range}",
        retry_attempts=5,
        retries_timeout=3600,
        retries_wait_initial=60,
        retries_wait_max=240,
    )
    return result


def fetch_tvl(
    projects: list[L2BeatProject],
    query_range: str,
    session: requests.Session | None = None,
):
    session = session or new_session()

    # Call api
    all_data = run_concurrently(
        function=lambda x: fetch_project_data(x, session, query_range),
        targets=projects,
        max_workers=2,
    )

    # Convert to dataframe
    return apply_schema(all_data, TVL_SCHEMA)
