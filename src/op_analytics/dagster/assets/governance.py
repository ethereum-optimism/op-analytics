from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def public_bucket(context: OpExecutionContext):
    """Pull Agora data."""
    from op_analytics.datasources.governance import execute

    result = execute.execute_pull()
    context.log.info(result)
