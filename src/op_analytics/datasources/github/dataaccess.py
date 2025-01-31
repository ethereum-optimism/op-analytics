from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Github(DailyDataset):
    """
    Supported GitHub datasets, paralleling the structure used by
    other third-party datasets (like DefiLlama, etc.).
    """

    # Initial github data pull following Bella's Hex notebook.
    TRAFFIC_METRICS = "repo_metrics_v1"
    REFERRER_METRICS = "repo_referrers_v1"

    # Github data brought in for platform metrics dashboards by Dennis.
    ISSUES = "github_issues_v1"
    PRS = "github_prs_v1"
    PR_COMMENTS = "github_pr_comments_v1"
    PR_REVIEWS = "github_pr_reviews_v2"
