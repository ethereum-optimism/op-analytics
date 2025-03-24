from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from ..dataaccess import Github
from .allrepos import GithubActivityData

log = structlog.get_logger()


def execute_pull():
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
        sort_by=["repo", "pr_number", "submitted_at"],
    )

    return {
        "prs": dt_summary(data.prs),
        "issues": dt_summary(data.issues),
        "pr_comments": dt_summary(data.pr_comments),
        "pr_reviews": dt_summary(data.pr_reviews),
    }
