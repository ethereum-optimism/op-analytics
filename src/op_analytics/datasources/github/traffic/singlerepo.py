from dataclasses import dataclass
from datetime import datetime

import polars as pl
import requests

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.dailydatautils import last_n_days
from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.time import datestr_subtract

log = structlog.get_logger()


# GitHub API endpoint
REPOS_BASE_URL = "https://api.github.com/repos/ethereum-optimism"


# Schema used for github repo metrics (long form).
METRIC_SCHEMA = {
    "dt": pl.Date(),
    "metric": pl.String(),
    "value": pl.Int32(),
}

REFERRERS_SCHEMA = {
    "referrer": pl.String(),
    "views": pl.Int32(),
    "unique_visitors": pl.Int32(),
}


@dataclass
class GithubRepoTrafficData:
    """Dataframes for a single repository"""

    # Metrics for all repositories. Concatenated in long form.
    metrics_df: pl.DataFrame

    # Referrers data for all repositories. Concatenated in long form.
    referrers_df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        session: requests.Session,
        headers: dict[str, str],
        repo: str,
        current_dt: str,
        views_and_clones_truncate: int,
        forks_truncate: int,
    ) -> "GithubRepoTrafficData":
        with bound_contextvars(repo=repo):
            views = get_data(
                session=session,
                url=REPOS_BASE_URL + f"/{repo}/traffic/views",
                headers=headers,
            )
            clones = get_data(
                session=session,
                url=REPOS_BASE_URL + f"/{repo}/traffic/clones",
                headers=headers,
            )
            forks = get_data(
                session=session,
                url=REPOS_BASE_URL + f"/{repo}/forks?sort=oldest",
                headers=headers,
            )
            referrers = get_data(
                session=session,
                url=REPOS_BASE_URL + f"/{repo}/traffic/popular/referrers",
                headers=headers,
            )

            views_df = process_views(views)
            clones_df = process_clones(clones)
            forks_df = process_forks(forks)

            # We use date N-1 as the reference dt for valid data.
            reference_dt = datestr_subtract(current_dt, delta_days=1)

            views_df_truncated = last_n_days(
                views_df,
                n_dates=views_and_clones_truncate,
                reference_dt=reference_dt,
            )
            clones_df_truncated = last_n_days(
                clones_df,
                n_dates=views_and_clones_truncate,
                reference_dt=reference_dt,
            )
            forks_df_truncated = last_n_days(
                forks_df,
                n_dates=forks_truncate,
                reference_dt=reference_dt,
            )

            metrics_df = (
                pl.concat(
                    [
                        views_df_truncated,
                        clones_df_truncated,
                        forks_df_truncated,
                    ]
                )
                .sort("dt")
                .with_columns(repo_name=pl.lit(repo))
            )

            referrers_df = process_referrers(referrers).with_columns(repo_name=pl.lit(repo))

            return cls(
                metrics_df=metrics_df,
                referrers_df=referrers_df,
            )


def process_views(views):
    views_data = []

    items = views["views"]
    log.info(f"process views: {len(items)} items")
    for item in items:
        dateval = datetime.fromisoformat(item["timestamp"]).date()
        views_data.append(
            {
                "dt": dateval,
                "metric": "views_total",
                "value": item["count"],
            }
        )
        views_data.append(
            {
                "dt": dateval,
                "metric": "views_unique",
                "value": item["uniques"],
            }
        )

    return pl.DataFrame(views_data, schema=METRIC_SCHEMA)


def process_clones(clones):
    clones_data = []

    items = clones["clones"]
    log.info(f"process clones: {len(items)} items")
    for item in clones["clones"]:
        dateval = datetime.fromisoformat(item["timestamp"]).date()
        clones_data.append(
            {
                "dt": dateval,
                "metric": "clones_total",
                "value": item["count"],
            }
        )
        clones_data.append(
            {
                "dt": dateval,
                "metric": "clones_unique",
                "value": item["uniques"],
            }
        )

    return pl.DataFrame(clones_data, schema=METRIC_SCHEMA)


def process_forks(forks):
    forks_data = []
    log.info(f"process forks: {len(forks)} items")
    for item in forks:
        forks_data.append(
            {
                "dt": datetime.fromisoformat(item["created_at"]).date(),
            }
        )

    forks_df = pl.DataFrame(forks_data, schema={"dt": pl.Date()})
    forks_count_df = (
        forks_df.group_by("dt")
        .agg(pl.count().alias("value"))
        .with_columns(metric=pl.lit("forks_total"))
    )
    return forks_count_df.select(
        pl.col("dt"),
        pl.col("metric"),
        pl.col("value").cast(pl.Int32()),
    )


def process_referrers(referrers):
    referrers_data = []
    log.info(f"process forks: {len(referrers)} items")
    for item in referrers:
        referrers_data.append(
            {
                "referrer": item["referrer"],
                "views": item["count"],
                "unique_visitors": item["uniques"],
            }
        )

    return pl.DataFrame(
        referrers_data,
        schema=REFERRERS_SCHEMA,
    )
