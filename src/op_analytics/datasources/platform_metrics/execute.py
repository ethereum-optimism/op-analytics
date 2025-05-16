from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from .dataaccess import PlatformMetrics
from .pg_daily_pull import PostgresDailyPull
from .prometheus_daily_pull import PrometheusDailyPull
from .bigquery import write_pg_to_bq, write_prom_to_bq

log = structlog.get_logger()


def execute_pull():
    data_pg = PostgresDailyPull.fetch()
    data_prom = PrometheusDailyPull.fetch()

    summary = {}
    summary["bigquery"] = {"pg": write_pg_to_bq(data_pg), "prom": write_prom_to_bq(data_prom)}

    PlatformMetrics.JOBS.write(
        dataframe=data_pg.jobs_df,
        sort_by=["dt", "metric"],
    )

    PlatformMetrics.PROMETHEUS_METRICS.write(
        dataframe=data_prom.metrics_df,
        sort_by=["dt", "referrer"],
    )

    summary["gcs"] = {
        "jobs_df": dt_summary(data_pg.jobs_df_truncated),
        "metrics_df": dt_summary(data_prom.metrics_df_truncated),
    }

    return summary
