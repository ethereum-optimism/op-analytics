from dagster import OpExecutionContext, asset
from op_analytics.datasources.github.dataaccess import Github


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


@asset(deps=[traffic])
def traffic_views():
    """Clickhouse external tables over GCS data:

    - github_gcs.repo_metrics_v1
    - github_gcs.repo_referrers_v1
    """
    Github.TRAFFIC_METRICS.create_clickhouse_view()
    Github.REFERRER_METRICS.create_clickhouse_view()


@asset(deps=[activity])
def activity_views():
    """Clickhouse external tables over GCS data:

    - github_gcs.github_issues_v1
    - github_gcs.github_prs_v1
    - github_gcs.github_pr_comments_v1
    - github_gcs.github_pr_reviews_v1
    """
    Github.ISSUES.create_clickhouse_view()
    Github.PRS.create_clickhouse_view()
    Github.PR_COMMENTS.create_clickhouse_view()
    Github.PR_REVIEWS.create_clickhouse_view()


@asset(deps=[activity], group_name="github", name="repo_metrics")
def repo_metrics(context: OpExecutionContext) -> None:
    """Dagster asset to compute and write GitHub PR metrics data.

    This asset depends on the activity asset which pulls raw PR data.
    Computes and writes daily metrics about PR activity and performance across repos.
    """
    from op_analytics.datasources.github.metrics.execute import execute_pull_repo_metrics

    result = execute_pull_repo_metrics()
    context.log.info(result)


@asset(deps=[repo_metrics])
def repo_metrics_view():
    """Clickhouse external table over GCS data:

    - github_gcs.repo_metrics_v1

    Contains daily metrics about PR activity and performance per repository:
    - number_of_prs
    - avg_time_to_approval_days
    - avg_time_to_first_non_bot_comment_days
    - avg_time_to_merge_days
    - approval_ratio
    - avg_comments_per_pr
    - merged_ratio
    - active_contributors
    """
    Github.REPO_METRICS.create_clickhouse_view()
