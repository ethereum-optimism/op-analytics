from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class PlatformMetrics(DailyDataset):
    """
    Supported Platform metrics datasets (Postgres, CircleCI and Prometheus), paralleling the structure used by
    other third-party datasets (like DefiLlama, etc.).
    """

    # Raw jobs table from Postgres.
    JOBS = "platform_metrics_jobs_v1"

    # Raw tables for all CircleCI tables.
    # CIRCLECI_PIPELINES = "platform_metrics_circleci_pipelines_v1"

    # Raw tables for all Prometheus datasets.
    PROMETHEUS_METRICS = "platform_metrics_prometheus_metrics_v1"
