import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import (
    write_daily_data,
    DailyDataset,
)

log = structlog.get_logger()


class Github(DailyDataset):
    """
    Supported GitHub datasets, paralleling the structure used by
    other third-party datasets (like DefiLlama, etc.).
    """

    # Raw github data
    ISSUES = "github_issues_v1"
    PRS = "github_prs_v1"
    PR_COMMENTS = "github_pr_comments_v1"
    PR_REVIEWS = "github_pr_reviews_v1"

    # Github metrics
    TRAFFIC_METRICS = "repo_metrics_v1"
    REFERRER_METRICS = "repo_referrers_v1"
    PR_METRICS = "github_pr_metrics_v1"

    def write(self, dataframe: pl.DataFrame, sort_by: list[str] | None = None):
        """
        Write DataFrame to GCS (daily partitioned) using the root_path
        for the given dataset constant (e.g. COMMITS, ISSUES, etc.).
        """
        return write_daily_data(
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
        )
