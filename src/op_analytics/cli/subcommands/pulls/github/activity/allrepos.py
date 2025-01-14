from dataclasses import dataclass
from datetime import timedelta

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt, date_fromstr

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
        partition_dt: str | None = None,
        include_open: bool = True,
        closed_min_dt: str | None = None,
        closed_max_dt: str | None = None,
        repo_concurrent_workers: int = 4,
    ) -> "GithubActivityData":
        partition_dt = partition_dt or now_dt()
        closed_max_dt = closed_max_dt or partition_dt

        if closed_min_dt is None:
            closed_min = date_fromstr(partition_dt) - timedelta(days=CLOSED_ITEMS_LAST_N_DAYS)
            closed_min_dt = closed_min.strftime("%Y-%m-%d")

        log.info(
            "github activity fetch",
            include_open=include_open,
            closed_min_dt=closed_min_dt,
            closed_max_dt=closed_max_dt,
        )

        # Fetch analytics for all repos.
        repo_dfs: dict[str, GithubRepoActivityData] = run_concurrently(
            lambda repo: GithubRepoActivityData.fetch(
                repo=repo,
                partition_dt=partition_dt,
                include_open=include_open,
                closed_min_dt=closed_min_dt,
                closed_max_dt=closed_max_dt,
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
