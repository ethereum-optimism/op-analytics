from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def platform_metrics(context: OpExecutionContext):
    """Pull daily platform metrics from Postgres and Prometheus."""
    from op_analytics.datasources.platformmetrics import execute
    from op_analytics.datasources.platformmetrics.dataaccess import PlatformMetrics

    result = execute.execute_pull()
    context.log.info(result)

    PlatformMetrics.JOBS.create_bigquery_external_table()
    PlatformMetrics.PIPELINES.create_bigquery_external_table()
    PlatformMetrics.PROMETHEUS_METRICS.create_bigquery_external_table()
