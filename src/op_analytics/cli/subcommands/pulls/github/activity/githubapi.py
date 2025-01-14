import time
from dataclasses import dataclass
from datetime import timedelta
from threading import Lock

import polars as pl
from github import Auth, Github
from github.Repository import Repository

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.time import datetime_fromdt, parse_isoformat

from .comments import COMMENTS_SCHEMA, comment_to_row
from .issues import ISSUES_SCHEMA, issue_to_row
from .pullrequests import PRS_SCHEMA, pr_to_row
from .reviews import REVIEWS_SCHEMA, review_to_row

log = structlog.get_logger()

_GITHUB_CLIENT: Github | None = None
_INIT_LOCK = Lock()


def init_client():
    global _GITHUB_CLIENT

    with _INIT_LOCK:
        if _GITHUB_CLIENT is None:
            auth = Auth.Token(env_get("GITHUB_API_TOKEN"))
            _GITHUB_CLIENT = Github(auth=auth)
    return _GITHUB_CLIENT


# All the repos we analyze are owned by our github organization.
OP_ORG = "ethereum-optimism"


@dataclass
class OptimismRepo:
    # Name of the rpeo
    repo: str

    @property
    def path(self):
        return f"{OP_ORG}/{self.repo}"


def fetch_prs(
    repo: OptimismRepo,
    current_dt: str,
    last_n_days: int,
) -> pl.DataFrame:
    """Fetch the current state of pull requests."""
    g = init_client()
    repo_obj: Repository = g.get_repo(repo.path)

    with bound_contextvars(fetch="PULLS"):
        rows = fetch_prs_or_issues(
            paginator=repo_obj.get_pulls,
            to_row_func=pr_to_row,
            current_dt=current_dt,
            last_n_days=last_n_days,
        )
        return pl.DataFrame(rows, schema=PRS_SCHEMA)


def fetch_issues(
    repo: OptimismRepo,
    current_dt: str,
    last_n_days: int,
) -> pl.DataFrame:
    """Fetch the current state of issues."""
    g = init_client()
    repo_obj: Repository = g.get_repo(repo.path)

    with bound_contextvars(fetch="ISSUES"):
        rows = fetch_prs_or_issues(
            paginator=repo_obj.get_issues,
            to_row_func=issue_to_row,
            current_dt=current_dt,
            last_n_days=last_n_days,
        )
        return pl.DataFrame(rows, schema=ISSUES_SCHEMA)


def bulk_fetch_comments(repo: OptimismRepo, pr_list: list[int]) -> pl.DataFrame:
    """Fetch all comments for a list of pull requests."""
    log.info(f"fetching comments for {len(pr_list)} prs")
    rows = bulk_fetch_for_prs(repo, pr_list, fetch_comments)
    return pl.DataFrame(rows, schema=COMMENTS_SCHEMA)


def bulk_fetch_reviews(repo: OptimismRepo, pr_list: list[int]) -> pl.DataFrame:
    """Fetch all reviews for a list of pull requests."""
    log.info(f"fetching reviews for {len(pr_list)} prs")
    rows = bulk_fetch_for_prs(repo, pr_list, fetch_reviews)
    return pl.DataFrame(rows, schema=REVIEWS_SCHEMA)


def bulk_fetch_for_prs(repo: OptimismRepo, pr_list: list[int], fetch_func) -> list[dict]:
    """Fetch all review comments for a list of pull requests."""
    g = init_client()
    repo_obj: Repository = g.get_repo(repo.path)

    # The number of requests here might put us up against the Github API's rate limit.
    # See https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28

    # To avoid hitting the rate limit we run the requests at a rate that is lower than
    # 5000 per hour.

    rate_limit_seconds_per_request = 3600 / 5000  # 5k requests per hour

    rows = []
    for i, pr_number in enumerate(pr_list):
        result = fetch_func(repo_obj, pr_number)
        rows.extend(result)
        if i % 5 == 0 or i + 1 == len(pr_list):
            log.info(f"fetching data for prs. completed {i+1} of {len(pr_list)}")
        time.sleep(rate_limit_seconds_per_request)

    return rows


def fetch_comments(repo_obj: Repository, pr_number: int) -> list[dict]:
    """Fetch all comments for a single pull request."""

    pull = repo_obj.get_pull(number=pr_number)
    comments = []
    for comment in pull.get_comments():
        comments.append(comment_to_row(pr_number, comment))

    return comments


def fetch_reviews(repo_obj: Repository, pr_number: int) -> list[dict]:
    """Fetch all comments for a single pull request."""

    pull = repo_obj.get_pull(number=pr_number)
    comments = []
    for comment in pull.get_reviews():
        comments.append(review_to_row(pr_number, comment))

    return comments


def fetch_prs_or_issues(
    paginator,
    to_row_func,
    current_dt: str,
    last_n_days: int,
) -> list[dict]:
    """Helper function to fetch pull requests or issues.

    We fetch the following data:

    - All open.
    - Recently closed up until last N days with respect to current dt.

    The idea is that every time we fetch data we store all currently open items and get updates
    for ones that were recently closed.

    We assume that items that were recently closed will not change anymore going into the future
    so that we can stop re-fetching data for them after some time.

    The combination of separately fetching only open items and capping the closed ones at a
    certain date allows us to fetch data quickly. The pagination over the closed items is what
    would take the longes, but since it is capped we break out of it quickly.

    If we want to backfill data we can set the threshold time way back and that way we will
    paginate through all of the closed items.
    """

    threshold = datetime_fromdt(current_dt) - timedelta(days=last_n_days)
    assert threshold.tzinfo is None

    start_time = time.time()
    open_prs_response = list(paginator(state="open", sort="created", direction="desc"))
    log.info(f"fetched {len(open_prs_response)} open in {time.time() - start_time:.2f}s")

    open_prs = []
    for open_pr in open_prs_response:
        open_prs.append(to_row_func(open_pr))

    closed_prs = []
    start_time = time.time()
    for closed_pr in paginator(state="closed", sort="updated", direction="desc"):
        closed_at = parse_isoformat(closed_pr._rawData["closed_at"])
        if closed_at > threshold:
            closed_prs.append(to_row_func(closed_pr))
        else:
            break
    log.info(
        f"found {len(closed_prs)} closed after {threshold.date()} in {time.time() - start_time:.2f}s"
    )

    return open_prs + closed_prs
