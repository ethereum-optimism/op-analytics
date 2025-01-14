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
        cls, repo: str, current_dt: str, closed_items_last_n_days: int
    ) -> "GithubRepoActivityData":
        with bound_contextvars(repo=repo):
            repo_obj = OptimismRepo(repo)

            prs = fetch_prs(repo_obj, current_dt, closed_items_last_n_days)
            issues = fetch_issues(repo_obj, current_dt, closed_items_last_n_days)

            pr_comments = bulk_fetch_comments(repo_obj, prs["number"].to_list())
            reviews = bulk_fetch_reviews(repo_obj, prs["number"].to_list())

            extracols = dict(repo=pl.lit(repo), dt=pl.lit(current_dt))

            return cls(
                prs=prs.with_columns(**extracols),
                issues=issues.with_columns(**extracols),
                pr_comments=pr_comments.with_columns(**extracols),
                pr_reviews=reviews.with_columns(**extracols),
            )
