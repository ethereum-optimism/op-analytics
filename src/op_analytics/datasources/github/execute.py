from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from .activity.allrepos import GithubActivityData
from .dataaccess import Github
from .traffic.allrepos import GithubTrafficData
from .traffic.bigquery import write_traffic_to_bq

log = structlog.get_logger()


def execute_pull_traffic():
    data = GithubTrafficData.fetch()

    summary = {}
    summary["bigquery"] = write_traffic_to_bq(data)

    Github.TRAFFIC_METRICS.write(
        dataframe=data.all_metrics_df_truncated,
        sort_by=["dt", "metric"],
    )

    Github.REFERRER_METRICS.write(
        dataframe=data.referrers_snapshot_df,
        sort_by=["dt", "referrer"],
    )

    summary["gcs"] = {
        "metrics_df": dt_summary(data.all_metrics_df_truncated),
        "referrers_df": dt_summary(data.referrers_snapshot_df),
    }

    return summary


def execute_pull_activity():
    data: GithubActivityData = GithubActivityData.fetch()

    Github.PRS.write(
        dataframe=data.prs,
        sort_by=["repo", "number", "state", "updated_at"],
    )

    Github.ISSUES.write(
        dataframe=data.issues,
        sort_by=["repo", "number", "state", "updated_at"],
    )

    Github.PR_COMMENTS.write(
        dataframe=data.pr_comments,
        sort_by=["repo", "pr_number", "updated_at"],
    )

    Github.PR_REVIEWS.write(
        dataframe=data.pr_reviews,
        sort_by=["repo", "pr_number", "updated_at"],
    )

    return {
        "prs": dt_summary(data.prs),
        "issues": dt_summary(data.issues),
        "pr_comments": dt_summary(data.pr_comments),
        "pr_reviews": dt_summary(data.pr_reviews),
    }


def insert_to_clickhouse():
    # TODO: Implement this.
    return None
