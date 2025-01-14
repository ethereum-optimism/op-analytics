from dataclasses import dataclass


import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt
from op_analytics.coreutils.env.vault import env_get


from .singlerepo import GithubRepoTrafficData

log = structlog.get_logger()


# Repos to track
REPOS = [
    "supersim",
    "superchainerc20-starter",
    "optimism",
    "op-geth",
    "superchain-registry",
    "superchain-ops",
    "docs",
    "specs",
    "design-docs",
    "infra",
]


VIEWS_AND_CLONES_TRUNCATE_LAST_N_DAYS = 2
FORKS_TRUNCATE_LAST_N_DAYS = 3


@dataclass
class GithubTrafficData:
    """Traffic data for a single repo.

    Metrics
    =======

    We include the following metrics

     - views_total
     - views_unique
     - clones_total
     - clones_unique
     - forks_total

    For views and clones the Github APIs report the last 14 days (there are 15
    distinct dates in the result). To avoid issues with overwriting data we
    only keep the last 2 days of fully reported data.

    If we fetch on day N, we only keep dates N-1 and N-2. Days N and N-14 will be
    incomplete, so we discard them. The rest of the window is also discarded
    because we don't what to overwrite it unnecessarily.

    On the forks endpoint github reports all historicals so we don't have a
    similar windowing problem at day N-14. However data at day N still may be
    incomplete, so we discard it.

    Referrers
    =========

    The Github API does not breakdown referals by date. That makes analysis
    somewhat complicated since one has to manually take care of any reporting
    overlaps that may exist.

    The referrers df is a snapshot of the value reported by Github at fetch
    time. The "dt" column is the fetch date.
    """

    # Metrics for all repositories. Concatenated in long form.
    all_metrics_df_truncated: pl.DataFrame

    # Referrers data for all repositories. Concatenated in long form.
    referrers_snapshot_df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        current_dt: str | None = None,
        views_and_clones_truncate: int | None = None,
        forks_truncate: int | None = None,
    ):
        current_dt = current_dt or now_dt()
        views_and_clones_truncate = (
            views_and_clones_truncate or VIEWS_AND_CLONES_TRUNCATE_LAST_N_DAYS
        )
        forks_truncate = forks_truncate or FORKS_TRUNCATE_LAST_N_DAYS

        token = env_get("GITHUB_API_TOKEN")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"token {token}",
        }

        session = new_session()

        # Fetch analytics for all repos.
        repo_dfs = run_concurrently(
            lambda repo: GithubRepoTrafficData.fetch(
                session,
                headers,
                repo=repo,
                current_dt=current_dt,
                views_and_clones_truncate=views_and_clones_truncate,
                forks_truncate=forks_truncate,
            ),
            targets=REPOS,
            max_workers=3,
        )

        # Consolidate into one dataframe per table for all repos.
        all_metrics = []
        all_referrers = []
        for repo_df in repo_dfs.values():
            all_metrics.append(repo_df.metrics_df)
            all_referrers.append(repo_df.referrers_df)

        all_metrics_df_truncated = pl.concat(all_metrics).select(
            "dt",
            "repo_name",
            "metric",
            "value",
        )

        referrers_snapshot_df = (
            pl.concat(all_referrers)
            .select(
                "repo_name",
                "referrer",
                "views",
                "unique_visitors",
            )
            .with_columns(dt=pl.lit(current_dt))
        )

        return cls(
            all_metrics_df_truncated=all_metrics_df_truncated,
            referrers_snapshot_df=referrers_snapshot_df,
        )
