from dagster import OpExecutionContext, asset


@asset
def traffic(context: OpExecutionContext) -> None:
    from op_analytics.cli.subcommands.pulls.github import execute

    result = execute.execute_pull_traffic()
    context.log.info(result)


@asset
def activity(context: OpExecutionContext) -> None:
    from op_analytics.cli.subcommands.pulls.github import execute

    result = execute.execute_pull_activity()
    context.log.info(result)


@asset(deps=[traffic, activity])
def write_to_clickhouse(context: OpExecutionContext) -> None:
    from op_analytics.cli.subcommands.pulls.github import execute

    result = execute.insert_to_clickhouse()
    context.log.info(result)
