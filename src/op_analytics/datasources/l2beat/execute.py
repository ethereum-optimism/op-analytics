import polars as pl

from op_analytics.coreutils.bigquery.write import (
    most_recent_dates,
    overwrite_partitioned_table,
    overwrite_partitions_dynamic,
    overwrite_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT

from .dataaccess import L2Beat
from .projects import L2BeatProjects

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://l2beat.com/api/scaling/summary"

BQ_DATASET = "dailydata_l2beat"

SUMMARY_TABLE = "chain_summary"
TVL_TABLE = "tvl_history"
ACTIVITY_TABLE = "activity_history"

# Use "max" for backfill
# Otherwise use 30d to get 6hr data intervals
QUERY_RANGE = "30d"


def execute_pull():
    """Pull data from L2Beat.

    - Fetch the L2Beat summary endpoint.
    - For each project in the L2Beat summary fetch TVL (last 30 days).
    - Write all results to BigQuery.
    """
    session = new_session()

    projects: L2BeatProjects = L2BeatProjects.fetch(
        query_range=QUERY_RANGE,
        session=session,
    )

    # Patch for writing to BQ as L2Beat evolved the API from a single provider to a list of providers
    # Get column names
    cols = projects.df.columns
    providers_idx = cols.index("providers")

    # Create new column and remove old one
    mod_summary_df = projects.df.with_columns(
        pl.col("providers")
        .fill_null([])
        .list.join(",")
        .fill_null("")
        .alias("provider")
    ).drop("providers")

    # Reorder columns: insert 'provider' where 'providers' was
    cols.remove("providers")
    cols.insert(providers_idx, "provider")
    mod_summary_df = mod_summary_df.select(cols)

    # Write to BQ.
    # NOTE: For L2Beat we have used native BQ writes in the past, so keeping that approach.
    # We still write to GCS so that we have a marker created in our etl_monitor table which
    # can help us track when the data pull succeeded/failed.
    overwrite_unpartitioned_table(mod_summary_df, BQ_DATASET, f"{SUMMARY_TABLE}_latest")
    L2Beat.CHAIN_SUMMARY.write(mod_summary_df.with_columns(dt=pl.lit(DEFAULT_DT)))

    if QUERY_RANGE == "max":
        overwrite_partitioned_table(
            df=projects.tvl_df,
            dataset=BQ_DATASET,
            table_name=TVL_TABLE,
        )
        overwrite_partitioned_table(
            df=projects.activity_df,
            dataset=BQ_DATASET,
            table_name=ACTIVITY_TABLE,
        )

        L2Beat.TVL.write(projects.tvl_df)
        L2Beat.ACTIVITY.write(projects.activity_df)
    else:
        overwrite_partitions_dynamic(
            df=most_recent_dates(projects.tvl_df, n_dates=7),
            dataset=BQ_DATASET,
            table_name=TVL_TABLE,
        )
        overwrite_partitions_dynamic(
            df=most_recent_dates(projects.activity_df, n_dates=7),
            dataset=BQ_DATASET,
            table_name=ACTIVITY_TABLE,
        )

        L2Beat.TVL.write(most_recent_dates(projects.tvl_df, n_dates=7))
        L2Beat.ACTIVITY.write(most_recent_dates(projects.activity_df, n_dates=7))
    return {
        "summary": dt_summary(mod_summary_df),
        "tvl": dt_summary(projects.tvl_df),
        "activity": dt_summary(projects.activity_df),
    }
