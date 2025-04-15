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


@asset
def tvs(context: OpExecutionContext):
    """Pull data from L2Beat TVS."""
    from op_analytics.datasources.l2beat import executetvs
    from op_analytics.datasources.l2beat.dataaccess import L2Beat

    result = executetvs.execute_pull()
    context.log.info(result)

    L2Beat.TVS_BREAKDOWN.create_bigquery_external_table()
