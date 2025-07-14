import os

from op_analytics.coreutils.bigquery.write import (
    overwrite_unpartitioned_table,
    upsert_unpartitioned_table,
)
from op_analytics.coreutils.logger import structlog

from op_analytics.datasources.platformmetrics.pg_daily_pull import PostgresDailyPull
from op_analytics.datasources.platformmetrics.prometheus_daily_pull import PrometheusDailyPull

log = structlog.get_logger()

# BigQuery Dataset and Tables
BQ_DATASET = "platform_metrics"
ANALYTICS_TABLE_PG = "platform_metrics_jobs_v1"
ANALYTICS_TABLE_PROM = "platform_metrics_prometheus_metrics_v1"


def write_pg_to_bq(data: PostgresDailyPull):
    """Write to BigQuery.

    This operation will be retired soon. Data will move to GCS + Clickhouse.
    """
    jobs_df = data.jobs_df.rename({"dt": "date"})

    if os.environ.get("CREATE_TABLES") == "true":
        # Use with care. Should only really be used the
        # first time the table is created.
        overwrite_unpartitioned_table(
            df=jobs_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE_PG,
        )
    else:
        upsert_unpartitioned_table(
            df=jobs_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE_PG,
            unique_keys=["date", "id", "pipeline_id", "workflow_id"],
        )

    return {"jobs_df": len(jobs_df)}


def write_prom_to_bq(data: PrometheusDailyPull):
    """Write to BigQuery.

    This operation will be retired soon. Data will move to GCS + Clickhouse.
    """

    metrics_df = data.metrics_df.rename({"dt": "date"})

    if os.environ.get("CREATE_TABLES") == "true":
        # Use with care. Should only really be used the
        # first time the table is created.
        overwrite_unpartitioned_table(
            df=metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE_PROM,
        )
    else:
        upsert_unpartitioned_table(
            df=metrics_df,
            dataset=BQ_DATASET,
            table_name=ANALYTICS_TABLE_PROM,
            unique_keys=["date", "metric", "unix_time"],
        )

    return {"metrics_df": len(metrics_df)}
