from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def chains(context: OpExecutionContext):
    """Pull data from L2 beat.

    Writes to BQ. Need to update the logic to write to Clickhouse.
    """
    from op_analytics.datasources.l2beat import execute

    result = execute.execute_pull()
    context.log.info(result)
