from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def growthepie(context: OpExecutionContext):
    """Pull daily chain summary fundamentals from GrowThePie."""
    from op_analytics.datasources.growthepie import chains_daily_fundamentals

    result = chains_daily_fundamentals.execute_pull()
    context.log.info(result)


@asset
def l2beat(context: OpExecutionContext):
    """Pull data from L2 beat.

    Writes to BQ. Need to update the logic to write to Clickhouse.
    """
    from op_analytics.datasources import l2beat_legacy

    result = l2beat_legacy.pull_l2beat()
    context.log.info(result)


@asset
def agora(context: OpExecutionContext):
    """Pull Agora data."""
    from op_analytics.datasources.agora import public_gcs_bucket

    result = public_gcs_bucket.execute_pull()
    context.log.info(result)
