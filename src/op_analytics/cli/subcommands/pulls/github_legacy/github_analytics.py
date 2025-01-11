import os

from op_analytics.coreutils.bigquery.write import (
    overwrite_partition_static,
    overwrite_unpartitioned_table,
    upsert_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_date

from .allrepos import GithubAnalyticsData

log = structlog.get_logger()


# BigQuery Dataset and Tables
BQ_DATASET = "uploads_api"
ANALYTICS_TABLE = "github_daily_analytics"
REFERRERS_TABLE = "github_daily_referrers_snapshot"


def execute_pull():
    data = GithubAnalyticsData.fetch()

    # TODO:
    # write_to_bq(data)  # For this we need to rename dt -> date
    # write_to_gcs(data)
    # insert_clickhouse()


def write_to_bq(data: GithubAnalyticsData):
    """Write to BigQuery.

    This operation will be retired soon. Data will move to Clickhouse.
    """

    if os.environ.get("CREATE_TABLES") == "true":
        # Use with care. Should only really be used the
        # first time the table is created.
        overwrite_unpartitioned_table(
            df=data.all_metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE,
        )
    else:
        upsert_unpartitioned_table(
            df=data.all_metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE,
            unique_keys=["date", "repo_name", "metric"],
        )

    current_date = now_date()
    overwrite_partition_static(
        df=data.all_referrers_df,
        partition_dt=current_date,
        dataset=BQ_DATASET,
        table_name=REFERRERS_TABLE,
    )
