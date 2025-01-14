from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt

from .singlerepo import GithubRepoActivityData

log = structlog.get_logger()


# Repos to track
REPOS = [
    "optimism",
    "supersim",
    "superchainerc20-starter",
    "superchain-registry",
    "superchain-ops",
    "docs",
    "specs",
    "design-docs",
    "infra",
]


# Nuber of days to look back when fetching closed PRs or Issues.
CLOSED_ITEMS_LAST_N_DAYS = 7


@dataclass
class GithubActivityData:
    prs: pl.DataFrame
    issues: pl.DataFrame

    pr_comments: pl.DataFrame
    pr_reviews: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        current_dt: str | None = None,
        closed_items_last_n_days: int | None = None,
        repo_concurrent_workers: int = 4,
    ) -> "GithubActivityData":
        current_dt = current_dt or now_dt()
        closed_items_last_n_days = closed_items_last_n_days or CLOSED_ITEMS_LAST_N_DAYS

        # Fetch analytics for all repos.
        repo_dfs: dict[str, GithubRepoActivityData] = run_concurrently(
            lambda repo: GithubRepoActivityData.fetch(
                repo=repo,
                current_dt=current_dt,
                closed_items_last_n_days=closed_items_last_n_days,
            ),
            targets=REPOS,
            max_workers=repo_concurrent_workers,
        )

        # Consolidate into one dataframe per table for all repos.
        return cls(
            prs=pl.concat([_.prs for _ in repo_dfs.values()]),
            issues=pl.concat([_.issues for _ in repo_dfs.values()]),
            pr_comments=pl.concat([_.pr_comments for _ in repo_dfs.values()]),
            pr_reviews=pl.concat([_.pr_reviews for _ in repo_dfs.values()]),
        )
