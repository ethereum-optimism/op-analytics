from dagster import OpExecutionContext, asset


@asset
def traffic(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github import execute

    result = execute.execute_pull_traffic()
    context.log.info(result)


@asset
def activity(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github import execute

    result = execute.execute_pull_activity()
    context.log.info(result)


@asset(deps=[traffic, activity])
def write_to_clickhouse(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github import execute

    result = execute.insert_to_clickhouse()
    context.log.info(result)


@asset(deps=[activity], group_name="github", name="repo_metrics")
def repo_metrics(context: OpExecutionContext) -> None:
    """Dagster asset to compute and write GitHub PR metrics data.

    This asset depends on the activity asset which pulls raw PR data.
    Computes and writes daily metrics about PR activity and performance across repos.
    """
    from op_analytics.datasources.github.metrics.execute import execute_pull_repo_metrics

    result = execute_pull_repo_metrics()
    context.log.info(result)
