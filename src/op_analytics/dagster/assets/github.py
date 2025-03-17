from dagster import OpExecutionContext, asset


@asset
def traffic(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github.traffic import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def activity(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github.activity import execute

    result = execute.execute_pull()
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
