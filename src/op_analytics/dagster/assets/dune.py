from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def dextrades(context: OpExecutionContext):
    """Pull dex trades from Dune."""
    from op_analytics.datasources.dune.dextrades import execute_pull

    result = execute_pull()
    context.log.info(result)
