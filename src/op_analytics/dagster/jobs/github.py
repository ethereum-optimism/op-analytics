from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def github_data(context: OpExecutionContext) -> None:
    from op_analytics.cli.subcommands.pulls.github_legacy import github_analytics

    result = github_analytics.execute_pull()
    context.log.info(result)


@asset(deps=["github_data"])
def github_data_to_clickhouse(context: OpExecutionContext) -> None:
    from op_analytics.cli.subcommands.pulls.github_legacy import github_analytics

    result = github_analytics.execute_pull()
    context.log.info(result)
