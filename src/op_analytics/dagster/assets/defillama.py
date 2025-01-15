from dagster import OpExecutionContext, asset


@asset
def token_mappings_to_bq(context: OpExecutionContext):
    from op_analytics.datasources.defillama import token_mappings

    result = token_mappings.execute()
    context.log.info(result)


@asset
def volumes_fees_revenue(context: OpExecutionContext):
    from op_analytics.datasources.defillama.volume_fees_revenue import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset(deps=[volumes_fees_revenue])
def volumes_fees_revenue_to_clickhouse(context: OpExecutionContext):
    from op_analytics.datasources.defillama.volume_fees_revenue import execute

    result = execute.write_to_clickhouse()
    context.log.info(result)
