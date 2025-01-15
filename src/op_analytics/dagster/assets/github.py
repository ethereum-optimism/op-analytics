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


@asset(deps=[traffic])
def traffic_views():
    """Clickhouse external tables over GCS data:

    - github_gcs.repo_metrics_v1
    - github_gcs.repo_referrers_v1
    """
    from op_analytics.datasources.github.dataaccess import Github

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
    from op_analytics.datasources.github.dataaccess import Github

    Github.ISSUES.create_clickhouse_view()
    Github.PRS.create_clickhouse_view()
    Github.PR_COMMENTS.create_clickhouse_view()
    Github.PR_REVIEWS.create_clickhouse_view()
