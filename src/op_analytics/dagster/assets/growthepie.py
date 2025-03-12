from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def chains_fundamentals(context: OpExecutionContext):
    """Pull daily chain summary fundamentals from GrowThePie."""
    from op_analytics.datasources.growthepie import chains_daily_fundamentals

    result = chains_daily_fundamentals.execute_pull()
    context.log.info(result)
