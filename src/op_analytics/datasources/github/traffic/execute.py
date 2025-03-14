from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from ..dataaccess import Github
from .allrepos import GithubTrafficData
from .bigquery import write_traffic_to_bq

log = structlog.get_logger()


def execute_pull():
    data = GithubTrafficData.fetch()

    summary = {}
    summary["bigquery"] = write_traffic_to_bq(data)

    Github.TRAFFIC_METRICS.write(
        dataframe=data.all_metrics_df_truncated,
        sort_by=["dt", "metric"],
    )

    Github.REFERRER_METRICS.write(
        dataframe=data.referrers_snapshot_df,
        sort_by=["dt", "referrer"],
    )

    summary["gcs"] = {
        "metrics_df": dt_summary(data.all_metrics_df_truncated),
        "referrers_df": dt_summary(data.referrers_snapshot_df),
    }

    return summary
