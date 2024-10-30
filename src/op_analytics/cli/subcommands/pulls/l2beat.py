import time
from dataclasses import dataclass

import polars as pl
from op_coreutils.bigquery.write import (
    overwrite_partition_static,
    overwrite_partitions_dynamic,
    overwrite_table,
)
from op_coreutils.logger import structlog
from op_coreutils.request import new_session
from op_coreutils.threads import run_concurrently
from op_coreutils.time import now_date

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://l2beat.com/api/scaling/summary"

BQ_DATASET = "uploads_api"

SUMMARY_TABLE = "l2beat_daily_chain_summary"
TVL_TABLE = "l2beat_daily_tvl"

TVL_SCHEMA = {
    "timestamp": pl.Int64,
    "native": pl.Float64,
    "canonical": pl.Float64,
    "external": pl.Float64,
    "ethPrice": pl.Float64,
}


def get_data(session, url):
    """Helper function to reuse an existing HTTP session to fetch data from a URL."""
    start = time.time()
    resp = session.request(
        method="GET",
        url=url,
        headers={"Content-Type": "application/json"},
    ).json()
    log.info(f"Fetched from {url}: {time.time() - start:.2f} seconds")
    return resp


@dataclass(frozen=True)
class L2BeatProject:
    """A single project as referenced by L2Beat."""

    id: str
    slug: str

    # The L2Beat url used to fetch TVL for this project
    tvl_url: str


def pull():
    """Pull data from L2Beat.

    - Fetch the L2Beat summary endpoint.
    - For each project in the L2Beat summary fetch TVL (last 30 days).
    - Write all results to BigQuery.
    """
    # Call the summary endpoint
    session = new_session()
    summary = get_data(session, SUMMARY_ENDPOINT)
    projects_summary = list(summary["data"]["projects"].values())

    # Parse the summary and store as a dataframe.
    summary_df = pl.DataFrame(projects_summary)

    # Set up TVL data http requests.
    query_range = (
        "30d"  # the query range can be modified if we need to go back and backfill older data
    )

    projects = []
    for project_data in projects_summary:
        slug = project_data["slug"]

        projects.append(
            L2BeatProject(
                id=project_data["id"],
                slug=slug,
                tvl_url=f"https://l2beat.com/api/scaling/tvl/{slug}?range={query_range}",
            )
        )

    # Run requests concurrenetly.
    tvl_data = run_concurrently(lambda x: get_data(session, x.tvl_url), projects, max_workers=8)
    percent_success = 100.0 * sum(_["success"] for _ in tvl_data.values()) / len(tvl_data)
    if percent_success < 80:
        raise Exception("Failed to get L2Beat data for >80%% of chains")

    dfs = []
    for project, data in tvl_data.items():
        if data["success"]:
            chart_data = data["data"]["chart"]
            columns = chart_data["types"]
            values = chart_data["data"]
            schema = [(col, TVL_SCHEMA[col]) for col in columns]

            # Pick the last value for each date.
            project_tvl = (
                pl.DataFrame(values, schema=schema, orient="row")
                .with_columns(
                    id=pl.lit(project.id),
                    slug=pl.lit(project.slug),
                    dt=pl.from_epoch(pl.col("timestamp")).dt.strftime("%Y-%m-%d"),
                )
                .sort("timestamp")
                .group_by("dt", maintain_order=True)
                .last()
            )

            dfs.append(project_tvl)

    tvl_df = pl.concat(dfs)

    # Write summary to BQ.
    dt = now_date()
    overwrite_table(summary_df, BQ_DATASET, f"{SUMMARY_TABLE}_latest")
    overwrite_partition_static(summary_df, dt, BQ_DATASET, f"{SUMMARY_TABLE}_history")

    # Write TVL to BQ.
    overwrite_partitions_dynamic(tvl_df, BQ_DATASET, f"{TVL_TABLE}_history")

    return {"summary": summary_df, "tvl": tvl_df}
