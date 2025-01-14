import os

from op_analytics.coreutils.bigquery.write import (
    overwrite_partition_static,
    overwrite_unpartitioned_table,
    upsert_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_date

from .allrepos import GithubTrafficData

log = structlog.get_logger()


# BigQuery Dataset and Tables
BQ_DATASET = "uploads_api"
ANALYTICS_TABLE = "github_daily_analytics"
REFERRERS_TABLE = "github_daily_referrers_snapshot"


def write_traffic_to_bq(data: GithubTrafficData):
    """Write to BigQuery.

    This operation will be retired soon. Data will move to GCS + Clickhouse.
    """

    metrics_df = data.all_metrics_df_truncated.rename({"dt": "date"})
    referrers_df = data.referrers_snapshot_df.drop("dt")

    if os.environ.get("CREATE_TABLES") == "true":
        # Use with care. Should only really be used the
        # first time the table is created.
        overwrite_unpartitioned_table(
            df=metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE,
        )
    else:
        upsert_unpartitioned_table(
            df=metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE,
            unique_keys=["date", "repo_name", "metric"],
        )

    current_date = now_date()
    overwrite_partition_static(
        df=referrers_df,
        partition_dt=current_date,
        dataset=BQ_DATASET,
        table_name=REFERRERS_TABLE,
    )

    return {
        "metrics_df": len(metrics_df),
        "referrers_df": len(referrers_df),
    }
