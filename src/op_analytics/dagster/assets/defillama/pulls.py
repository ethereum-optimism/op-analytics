from dagster import OpExecutionContext, asset


@asset
def volumes_fees_revenue(context: OpExecutionContext):
    from op_analytics.cli.subcommands.pulls.defillama import volume_fees_revenue

    result = volume_fees_revenue.execute_pull()
    context.log.info(result)


@asset(deps=[volumes_fees_revenue])
def volumes_fees_revenue_to_clickhouse(context: OpExecutionContext):
    from op_analytics.cli.subcommands.pulls.defillama import volume_fees_revenue

    result = volume_fees_revenue.write_to_clickhouse()
    context.log.info(result)
