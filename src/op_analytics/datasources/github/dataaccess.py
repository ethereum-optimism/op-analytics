from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Github(DailyDataset):
    """
    Supported GitHub datasets, paralleling the structure used by
    other third-party datasets (like DefiLlama, etc.).
    """

    # Metrics provided by the Github Traffic and Referrers APIs.
    TRAFFIC_METRICS = "repo_metrics_v1"
    REFERRER_METRICS = "repo_referrers_v1"

    # Raw data for all tracked github repos.
    ISSUES = "github_issues_v1"
    PRS = "github_prs_v1"
    PR_COMMENTS = "github_pr_comments_v1"
    PR_REVIEWS = "github_pr_reviews_v2"

    # Computed metrics for the github repos (depend on raw data).
    REPO_METRICS = "github_repo_metrics_v1"
