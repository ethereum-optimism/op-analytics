from dagster import (
    OpExecutionContext,
    asset,
)


@asset(group_name="sugar")
def sugar_daily(context: OpExecutionContext) -> None:
    """Pull daily Sugar protocol data.

    Fetches and processes daily Sugar protocol metrics and stores them in our data warehouse.
    The data includes key protocol metrics like TVL, volume, and other relevant statistics.
    """
    from op_analytics.datasources.sugar import execute

    result = execute.execute_pull()
    context.log.info("Sugar daily pull completed", result=result)
