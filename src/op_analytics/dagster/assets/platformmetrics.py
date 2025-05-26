from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def platform_metrics(context: OpExecutionContext):
    """Pull daily platform metrics from Postgres and Prometheus."""
    from op_analytics.datasources.platform_metrics import execute

    result = execute.execute_pull()
    context.log.info(result)
