from op_analytics.coreutils.logger import structlog

from .allrepos import GithubAnalyticsData
from .bigquery import write_to_bq
from .gcs import write_to_gcs

log = structlog.get_logger()


def execute_pull():
    data = GithubAnalyticsData.fetch()

    summary = {}
    summary["bigquery"] = write_to_bq(data)
    summary["gcs"] = write_to_gcs(data)
    # write_to_gcs(data)
    # insert_clickhouse()
