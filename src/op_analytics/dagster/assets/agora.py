from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def public_bucket(context: OpExecutionContext):
    """Pull Agora data."""
    from op_analytics.datasources.agora import public_gcs_bucket

    result = public_gcs_bucket.execute_pull()
    context.log.info(result)
