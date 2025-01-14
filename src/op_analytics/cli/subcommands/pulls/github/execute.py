from op_analytics.coreutils.logger import structlog

from .activity.allrepos import GithubActivityData
from .dataaccess import GithubDataset
from .traffic.allrepos import GithubTrafficData
from .traffic.bigquery import write_traffic_to_bq

log = structlog.get_logger()


def execute_pull_traffic():
    data = GithubTrafficData.fetch()

    summary = {}
    summary["bigquery"] = write_traffic_to_bq(data)

    GithubDataset.TRAFFIC_METRICS.write(
        dataframe=data.all_metrics_df_truncated,
        sort_by=["dt", "metric"],
    )

    GithubDataset.REFERRER_METRICS.write(
        dataframe=data.referrers_snapshot_df,
        sort_by=["dt", "referrer"],
    )

    summary["gcs"] = {
        "metrics_df": len(data.all_metrics_df_truncated),
        "referrers_df": len(data.referrers_snapshot_df),
    }

    return summary


def execute_pull_activity():
    data: GithubActivityData = GithubActivityData.fetch(closed_items_last_n_days=10)

    GithubDataset.PRS.write(
        dataframe=data.prs,
        sort_by=["repo", "number", "state", "updated_at"],
    )

    GithubDataset.ISSUES.write(
        dataframe=data.issues,
        sort_by=["repo", "number", "state", "updated_at"],
    )

    GithubDataset.PR_COMMENTS.write(
        dataframe=data.pr_comments,
        sort_by=["repo", "pr_number", "updated_at"],
    )

    GithubDataset.PR_REVIEWS.write(
        dataframe=data.pr_reviews,
        sort_by=["repo", "pr_number", "updated_at"],
    )

    return {
        "prs": len(data.prs),
        "issues": len(data.issues),
        "pr_comments": len(data.pr_comments),
        "pr_reviews": len(data.pr_reviews),
    }


def insert_to_clickhouse():
    # TODO: Implement this.
    return None
