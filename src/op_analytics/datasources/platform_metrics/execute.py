from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from op_analytics.datasources.platform_metrics.dataaccess import PlatformMetrics
from op_analytics.datasources.platform_metrics.pg_daily_pull import PostgresDailyPull
from op_analytics.datasources.platform_metrics.prometheus_daily_pull import PrometheusDailyPull
from op_analytics.datasources.platform_metrics.bigquery import write_pg_to_bq, write_prom_to_bq
import polars as pl
from op_analytics.coreutils.bigquery.write import (
    most_recent_dates,
    overwrite_partitioned_table,
    overwrite_partitions_dynamic,
    overwrite_unpartitioned_table,
)

log = structlog.get_logger()


def execute_pull():
    data_pg = PostgresDailyPull.fetch()
    data_prom = PrometheusDailyPull.fetch()

    summary = {}
    summary["bigquery"] = {"pg": write_pg_to_bq(data_pg), "prom": write_prom_to_bq(data_prom)}

    PlatformMetrics.JOBS.write(
        dataframe=data_pg.jobs_df,
        sort_by=["dt", "id"],
    )

    PlatformMetrics.PROMETHEUS_METRICS.write(
        dataframe=data_prom.metrics_df,
        sort_by=["dt", "metric"],
    )

    summary["gcs"] = {
        "jobs_df": dt_summary(data_pg.jobs_df),
        "metrics_df": dt_summary(data_prom.metrics_df),
    }

    return summary


# BigQuery Dataset and Tables
BQ_DATASET = "platform_metrics"
ANALYTICS_TABLE_PG = "platform_metrics_jobs_v1"
ANALYTICS_TABLE_PROM = "platform_metrics_prometheus_metrics_v1"

# Use "max" for backfill
# Otherwise use 30d to get 6hr data intervals
QUERY_RANGE = "max"


def execute_pull_1():
    """Pull data from L2Beat.

    - Fetch the L2Beat summary endpoint.
    - For each project in the L2Beat summary fetch TVL (last 30 days).
    - Write all results to BigQuery.
    """

    data_pg = PostgresDailyPull.fetch()
    data_prom = PrometheusDailyPull.fetch()

    pg: PostgresDailyPull = PostgresDailyPull.fetch()
    prometheus: PrometheusDailyPull = PrometheusDailyPull.fetch()

    # Write to BQ.
    # NOTE: For L2Beat we have used native BQ writes in the past, so keeping that approach.
    # We still write to GCS so that we have a marker created in our etl_monitor table which
    # can help us track when the data pull succeded/failed.
    overwrite_unpartitioned_table(pg.jobs_df, BQ_DATASET, f"{ANALYTICS_TABLE_PG}")
    overwrite_unpartitioned_table(prometheus.metrics_df, BQ_DATASET, f"{ANALYTICS_TABLE_PROM}")
    # PlatformMetrics.JOBS.write(pg.jobs_df.with_columns(dt=pl.lit("dt")))
    # PlatformMetrics.PROMETHEUS_METRICS.write(prometheus.metrics_df.with_columns(dt=pl.lit("dt")))

    # if QUERY_RANGE == "max":
    #     overwrite_partitioned_table(
    #         df=projects.tvl_df,
    #         dataset=BQ_DATASET,
    #         table_name=TVL_TABLE,
    #     )
    #     overwrite_partitioned_table(
    #         df=projects.activity_df,
    #         dataset=BQ_DATASET,
    #         table_name=ACTIVITY_TABLE,
    #     )

    #     L2Beat.TVL.write(projects.tvl_df)
    #     L2Beat.ACTIVITY.write(projects.activity_df)
    # else:
    #     overwrite_partitions_dynamic(
    #         df=most_recent_dates(projects.tvl_df, n_dates=7),
    #         dataset=BQ_DATASET,
    #         table_name=TVL_TABLE,
    #     )
    #     overwrite_partitions_dynamic(
    #         df=most_recent_dates(projects.activity_df, n_dates=7),
    #         dataset=BQ_DATASET,
    #         table_name=ACTIVITY_TABLE,
    #     )

    #     L2Beat.TVL.write(most_recent_dates(projects.tvl_df, n_dates=7))
    #     L2Beat.ACTIVITY.write(most_recent_dates(projects.activity_df, n_dates=7))
    return {"jobs": dt_summary(pg.jobs_df), "prometheus_metrics": dt_summary(prometheus.metrics_df)}
