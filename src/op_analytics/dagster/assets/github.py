from dagster import OpExecutionContext, asset

from op_analytics.transforms.main import execute_dt_transforms


@asset
def traffic(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github.traffic import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset
def activity_raw(context: OpExecutionContext) -> None:
    from op_analytics.datasources.github.activity import execute

    result = execute.execute_pull()
    context.log.info(result)


@asset(deps=[activity_raw])
def activity_metrics(context: OpExecutionContext) -> None:
    """Dagster asset to compute and write GitHub PR metrics data.

    This asset depends on the activity asset which pulls raw PR data.
    Computes and writes daily metrics about PR activity and performance across repos.
    """
    from op_analytics.datasources.github.metrics.execute import execute_pull_repo_metrics

    result = execute_pull_repo_metrics()
    context.log.info(result)


@asset(deps=[activity_raw])
def transforms_github(context: OpExecutionContext):
    """Execute github transforms.

    The github transforms include:

    - User reviews fact table.
    """
    result = execute_dt_transforms(group_name="github", force_complete=True)
    context.log.info(result)
