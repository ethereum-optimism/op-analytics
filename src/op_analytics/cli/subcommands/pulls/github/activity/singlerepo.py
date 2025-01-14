from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog

from .githubapi import (
    OptimismRepo,
    bulk_fetch_comments,
    bulk_fetch_reviews,
    fetch_issues,
    fetch_prs,
)

log = structlog.get_logger()


@dataclass
class GithubRepoActivityData:
    """Dataframes for a single repository.

    NOTE: The notebooks created by Dennis were also fetching commits and releases.
    We aren't fetching those for now. We can do so if the data is needed.
    """

    prs: pl.DataFrame
    issues: pl.DataFrame

    pr_comments: pl.DataFrame
    pr_reviews: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        repo: str,
        include_open: bool,
        partition_dt: str,
        closed_min_dt: str,
        closed_max_dt: str,
    ) -> "GithubRepoActivityData":
        with bound_contextvars(repo=repo):
            repo_obj = OptimismRepo(repo)

            prs = fetch_prs(
                repo_obj,
                include_open=include_open,
                closed_min_dt=closed_min_dt,
                closed_max_dt=closed_max_dt,
            )
            issues = fetch_issues(
                repo_obj,
                include_open=include_open,
                closed_min_dt=closed_min_dt,
                closed_max_dt=closed_max_dt,
            )

            # For all the fetched prs also fetch comments and reviews.
            pr_list = prs["number"].to_list()
            pr_comments = bulk_fetch_comments(repo_obj, pr_list)
            pr_reviews = bulk_fetch_reviews(repo_obj, pr_list)

            extracols = dict(repo=pl.lit(repo), dt=pl.lit(partition_dt))

            return cls(
                prs=prs.with_columns(**extracols),
                issues=issues.with_columns(**extracols),
                pr_comments=pr_comments.with_columns(**extracols),
                pr_reviews=pr_reviews.with_columns(**extracols),
            )
