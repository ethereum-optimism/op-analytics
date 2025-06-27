from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from op_analytics.datasources.platformmetrics.dataaccess import PlatformMetrics
from op_analytics.datasources.platformmetrics.pg_daily_pull import PostgresDailyPull
from op_analytics.datasources.platformmetrics.prometheus_daily_pull import PrometheusDailyPull

log = structlog.get_logger()


def execute_pull():
    data_pg = PostgresDailyPull.fetch()
    data_prom = PrometheusDailyPull.fetch()

    summary = {}

    PlatformMetrics.JOBS.write(
        dataframe=data_pg.jobs_df,
        sort_by=["dt", "id"],
    )

    PlatformMetrics.PIPELINES.write(
        dataframe=data_pg.pipelines_df,
        sort_by=["dt", "id"],
    )

    PlatformMetrics.PROMETHEUS_METRICS.write(
        dataframe=data_prom.metrics_df,
        sort_by=["dt", "metric"],
    )

    summary["gcs"] = {
        "jobs_df": dt_summary(data_pg.jobs_df),
        "pipelines_df": dt_summary(data_pg.pipelines_df),
        "metrics_df": dt_summary(data_prom.metrics_df),
    }

    return summary
