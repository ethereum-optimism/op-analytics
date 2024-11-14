import os
import requests
from datetime import datetime

import polars as pl
from op_coreutils.bigquery.write import (
    overwrite_partition_static,
    overwrite_unpartitioned_table,
    upsert_unpartitioned_table,
)
from op_coreutils.logger import structlog, bound_contextvars
from op_coreutils.request import new_session, get_data
from op_coreutils.time import now_date
from op_coreutils.threads import run_concurrently
from op_coreutils.env.vault import env_get

log = structlog.get_logger()

# GitHub API endpoint
REPOS_BASE_URL = "https://api.github.com/repos/ethereum-optimism"

# Repos to track
REPOS = [
    "supersim",
    "superchainerc20-starter",
    "optimism",
    "op-geth",
    "superchain-registry",
    "superchain-ops",
    "docs",
    "specs",
    "design-docs",
    "infra",
]

# Dataset and Tables
BQ_DATASET = "uploads_api"
ANALYTICS_TABLE = "github_daily_analytics"
REFERRERS_TABLE = "github_daily_referrers_snapshot"


def pull():
    session = new_session()

    token = env_get("GITHUB_API_TOKEN")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"token {token}",
    }

    # Fetch data for all repos.
    repo_dfs = run_concurrently(
        lambda repo: process_repo(session, headers, repo=repo),
        targets=REPOS,
        max_workers=3,
    )

    # Consolidate into one dataframe per table for all repos.
    all_metrics = []
    all_referrers = []
    for referrers_df, metrics_df in repo_dfs.values():
        all_metrics.append(metrics_df)
        all_referrers.append(referrers_df)
    all_metrics_df = pl.concat(all_metrics).select(
        "date",
        "repo_name",
        "metric",
        "value",
    )
    all_referrers_df = pl.concat(all_referrers).select(
        "repo_name",
        "referrer",
        "views",
        "unique_visitors",
    )

    # Write to BQ.
    if os.environ.get("CREATE_TABLES") == "true":
        # Use with care. Should only really be used the
        # first time the table is created.
        overwrite_unpartitioned_table(
            df=all_metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE,
        )
    else:
        upsert_unpartitioned_table(
            df=all_metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE,
            unique_keys=["date", "repo_name", "metric"],
        )

    current_date = now_date()
    overwrite_partition_static(
        df=all_referrers_df,
        partition_dt=current_date,
        dataset=BQ_DATASET,
        table_name=REFERRERS_TABLE,
    )


def process_repo(session: requests.Session, headers: dict[str, str], repo: str):
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
        metrics_df = (
            pl.concat([views_df, clones_df, forks_df])
            .sort("date")
            .with_columns(repo_name=pl.lit(repo))
        )

        referrers_df = process_referrers(referrers).with_columns(repo_name=pl.lit(repo))

        return referrers_df, metrics_df


METRIC_SCHEMA = {
    "date": pl.Date(),
    "metric": pl.String(),
    "value": pl.Int32(),
}


def process_views(views):
    views_data = []

    items = views["views"]
    log.info(f"process views: {len(items)} items")
    for item in items:
        dateval = datetime.fromisoformat(item["timestamp"]).date()
        views_data.append(
            {
                "date": dateval,
                "metric": "views_total",
                "value": item["count"],
            }
        )
        views_data.append(
            {
                "date": dateval,
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
                "date": dateval,
                "metric": "clones_total",
                "value": item["count"],
            }
        )
        clones_data.append(
            {
                "date": dateval,
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
                "date": datetime.fromisoformat(item["created_at"]).date(),
            }
        )

    forks_df = pl.DataFrame(forks_data, schema={"date": pl.Date()})
    forks_count_df = (
        forks_df.group_by("date")
        .agg(pl.count().alias("value"))
        .with_columns(metric=pl.lit("forks_total"))
    )
    return forks_count_df.select(
        pl.col("date"),
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
        schema={
            "referrer": pl.String(),
            "views": pl.Int32(),
            "unique_visitors": pl.Int32(),
        },
    )
