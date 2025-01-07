import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import polars as pl
import requests

from op_analytics.cli.subcommands.pulls.github.dataaccess import GitHubPlatform
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import (
    now,
    parse_isoformat,
    datetime_fromepoch,
    timezone,
)

log = structlog.get_logger()


def get_rate_limit_info(session: requests.Session, token: Optional[str] = None) -> dict:
    """
    Fetch the current GitHub rate limit info from /rate_limit endpoint.
    Returns a dict like: { 'limit': ..., 'remaining': ..., 'reset': ... } for the 'core' resource,
    where 'reset' is an integer epoch.
    """
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    resp = session.get("https://api.github.com/rate_limit", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    core_data = data["resources"]["core"]
    reset_value = core_data["reset"]

    return {
        "limit": core_data["limit"],
        "remaining": core_data["remaining"],
        "reset": reset_value,  # store as integer epoch
    }


def wait_for_rate_limit(session: requests.Session, token: Optional[str] = None) -> None:
    """
    Checks the 'core' REST rate limit. If remaining=0, waits until the reset epoch.
    """
    while True:
        rate_info = get_rate_limit_info(session, token=token)
        remaining = rate_info["remaining"]
        reset_epoch = rate_info["reset"]
        now_utc = now()

        log.info(
            "GitHub Rate Limit",
            current_utc=str(now_utc),
            remaining=remaining,
            reset_epoch=reset_epoch,
        )

        if remaining == 0:
            reset_dt = datetime_fromepoch(reset_epoch)
            wait_seconds = (reset_dt - now_utc).total_seconds()
            if wait_seconds > 0:
                log.warning(
                    "[wait_for_rate_limit] Exhausted. Waiting.",
                    wait_time_sec=wait_seconds,
                    reset_time=str(reset_dt),
                )
                time.sleep(wait_seconds + 2)
            else:
                log.info("[wait_for_rate_limit] Presumably just reset, proceeding.")
        else:
            break


@dataclass
class GitHubData:
    """
    Container for GitHub data frames.
    """

    commits: pl.DataFrame
    issues: pl.DataFrame
    pulls: pl.DataFrame
    releases: pl.DataFrame
    pr_comments: pl.DataFrame


class GitHubFetcher:
    """
    Fetch data from the GitHub API using a single session and concurrency utilities.
    """

    def __init__(self, token: str, owner: str, repos: List[str]):
        self.token = token
        self.owner = owner
        self.repos = repos
        self.session = new_session()
        self.session.headers.update({"Authorization": f"token {self.token}"})

    def _fetch_single_page(self, url: str) -> list | dict:
        """
        Attempt to fetch a single page, calling wait_for_rate_limit and retrying
        up to max_attempts times with exponential backoff.
        """
        max_attempts = 5
        attempt = 0
        backoff_seconds = 5

        while True:
            attempt += 1
            try:
                wait_for_rate_limit(self.session, token=self.token)
                return get_data(session=self.session, url=url, retry_attempts=1)
            except Exception as exc:
                log.warning(
                    "Error fetching single page",
                    url=url,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    error=str(exc),
                )
                if attempt >= max_attempts:
                    log.error("Exceeded max fetch attempts, re-raising exception.", url=url)
                    raise
                else:
                    time.sleep(backoff_seconds)
                    backoff_seconds *= 2

    def _fetch_all_pages(self, initial_url: str) -> List[dict]:
        """
        Fetch all pages for a given endpoint:
        1) Fetch the initial page (serially).
        2) Parse the 'Link' header to determine pagination.
        3) Fetch subsequent pages in parallel.
        """
        first_page_data = self._fetch_single_page(initial_url)
        first_page_data = (
            first_page_data if isinstance(first_page_data, list) else [first_page_data]
        )
        all_items = first_page_data.copy()

        head_resp = self.session.head(initial_url)
        link_header = head_resp.headers.get("Link", None)
        if not link_header:
            return all_items

        last_link = None
        for part in link_header.split(","):
            if 'rel="last"' in part:
                start = part.find("<") + 1
                end = part.find(">")
                last_link = part[start:end]
                break

        if not last_link:
            next_link = None
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    start = part.find("<") + 1
                    end = part.find(">")
                    next_link = part[start:end]
                    break
            if next_link:
                extra_data = self._fetch_single_page(next_link)
                extra_data = extra_data if isinstance(extra_data, list) else [extra_data]
                all_items.extend(extra_data)
            return all_items

        from urllib.parse import parse_qs, urlparse

        parsed_last = urlparse(last_link)
        qs_last = parse_qs(parsed_last.query)
        last_page = int(qs_last.get("page", [1])[0])
        if last_page <= 1:
            return all_items

        parsed_init = urlparse(initial_url)
        qs_init = parse_qs(parsed_init.query)
        qs_init.pop("page", None)

        def build_page_url(page_num: int) -> str:
            from urllib.parse import urlencode

            new_qs = qs_init.copy()
            new_qs["page"] = str(page_num)
            query_str = urlencode(new_qs, doseq=True)
            return f"{parsed_init.scheme}://{parsed_init.netloc}{parsed_init.path}?{query_str}"

        all_page_urls = [build_page_url(p) for p in range(2, last_page + 1)]

        def fetch_func(url: str) -> List[dict]:
            data = self._fetch_single_page(url)
            return data if isinstance(data, list) else [data]

        targets = {f"page_{idx+2}": url for idx, url in enumerate(all_page_urls)}
        concurrency_results = run_concurrently(fetch_func, targets=targets, max_workers=4)

        for _, result_list in concurrency_results.items():
            all_items.extend(result_list)

        return all_items

    def fetch_commits(self, repo: str, since: Optional[str] = None) -> List[dict]:
        url = f"https://api.github.com/repos/{self.owner}/{repo}/commits?state=all"
        if since:
            url += f"&since={since}"
        return self._fetch_all_pages(url)

    def fetch_issues(self, repo: str, since: Optional[str] = None) -> List[dict]:
        url = f"https://api.github.com/repos/{self.owner}/{repo}/issues?state=all"
        if since:
            url += f"&since={since}"
        return self._fetch_all_pages(url)

    def fetch_pulls(self, repo: str, since: Optional[str] = None) -> List[dict]:
        """
        Corrected fetch_pulls method that appends pull request parameters
        to the proper endpoint.
        """
        url = (
            f"https://api.github.com/repos/{self.owner}/{repo}/pulls"
            "?state=all&sort=updated&direction=desc"
        )
        if since:
            url += f"&since={since}"
        return self._fetch_all_pages(url)

    def fetch_releases(self, repo: str) -> List[dict]:
        url = f"https://api.github.com/repos/{self.owner}/{repo}/releases?state=all"
        return self._fetch_all_pages(url)

    def fetch_pr_comments(self, repo: str, pr_num: int) -> List[dict]:
        url = f"https://api.github.com/repos/{self.owner}/{repo}/pulls/{pr_num}/comments"
        return self._fetch_all_pages(url)

    def fetch_pr_reviews(self, repo: str, pr_num: int) -> List[dict]:
        url = f"https://api.github.com/repos/{self.owner}/{repo}/pulls/{pr_num}/reviews"
        return self._fetch_all_pages(url)


def _extract_approval_info(reviews: List[dict]) -> Tuple[pl.Datetime, Optional[str]]:
    """
    Find the first 'APPROVED' review's time and user.
    Returns (approval_datetime, reviewer_login).
    """
    approval = next((r for r in reviews if r.get("state", "").upper() == "APPROVED"), None)
    if approval:
        submitted_at = parse_isoformat(approval["submitted_at"].replace("Z", "+00:00"))
        return (submitted_at, approval["user"]["login"] if approval.get("user") else None)
    # Use epoch=0 to simulate 'datetime.min'
    return (datetime_fromepoch(0), None)


def _comments_to_df(comments: List[dict]) -> pl.DataFrame:
    """
    Convert a list of comment dicts to a polars DataFrame.
    """
    if not comments:
        return pl.DataFrame(
            schema={"user": pl.Utf8, "created_at": pl.Datetime, "is_bot": pl.Boolean}
        )

    df = pl.DataFrame(
        {
            "user": [c["user"].get("login", None) if c.get("user") else None for c in comments],
            "created_at": [c["created_at"] for c in comments],
        }
    ).with_columns(
        pl.col("created_at")
        .str.replace("Z$", "+00:00")
        .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("user").fill_null(""),
    )
    df = df.with_columns(pl.col("user").str.contains(r"(?i)\[bot\]").alias("is_bot"))
    return df


def _process_pull_details(
    pulls_df: pl.DataFrame,
    fetcher: GitHubFetcher,
    since: Optional[str] = None,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Handle pull request details, fetching concurrency results,
    and computing metrics (time-to-merge, time-to-approval, etc.).
    """

    if pulls_df.is_empty():
        return pulls_df, pl.DataFrame()

    for col_name in ["created_at", "merged_at"]:
        if col_name not in pulls_df.columns:
            pulls_df = pulls_df.with_columns(pl.lit(None).alias(col_name))

    pulls_df = pulls_df.with_columns(
        pl.col("created_at").cast(pl.Utf8).fill_null(""),
        pl.col("merged_at").cast(pl.Utf8).fill_null(""),
    ).with_columns(
        pl.col("created_at")
        .str.replace("Z$", "+00:00")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("merged_at")
        .str.replace("Z$", "+00:00")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
    )

    if since:
        try:
            since_dt = parse_isoformat(since).replace(tzinfo=timezone.utc)
            pulls_df = pulls_df.filter(pl.col("created_at") >= since_dt)
        except ValueError:
            log.warning("Invalid 'since' value, ignoring filter", since=since)

    if pulls_df.is_empty():
        return pulls_df, pl.DataFrame()

    pulls_list = pulls_df.to_dicts()
    targets = {f"pr_{i}": pr_dict for i, pr_dict in enumerate(pulls_list)}

    def fetch_pr_data(pr_info: dict) -> dict:
        """
        Fetch reviews & comments for a single PR; compute earliest APPROVED time.
        """
        repo = pr_info.get("repo")
        pr_number = pr_info.get("number")
        idx = pr_info.get("_idx", -1)

        if pr_number is None:
            return {
                "idx": idx,
                "approval_time": None,
                "approver": None,
                "comments": [],
            }

        reviews = fetcher.fetch_pr_reviews(repo, pr_number)
        approval_dt, approver = _extract_approval_info(reviews)
        raw_comments = fetcher.fetch_pr_comments(repo, pr_number)

        return {
            "idx": idx,
            "approval_time": approval_dt if approval_dt != datetime_fromepoch(0) else None,
            "approver": approver,
            "comments": raw_comments,
        }

    concurrency_results = run_concurrently(fetch_pr_data, targets, max_workers=8)

    comments_accum: list[dict] = []
    updated_approval_times = [None] * len(targets)
    updated_approvers = [None] * len(targets)

    for k, fetched_data in concurrency_results.items():
        i = int(k.replace("pr_", ""))
        updated_approval_times[i] = fetched_data["approval_time"]
        updated_approvers[i] = fetched_data["approver"]

        orig_pr_info = targets[k]
        for c in fetched_data["comments"] or []:
            c["repo"] = orig_pr_info.get("repo")
            c["pull_number"] = orig_pr_info.get("number")
        comments_accum.extend(fetched_data["comments"] or [])

    pulls_df = pulls_df.with_columns(
        pl.Series("approval_time", updated_approval_times),
        pl.Series("approver", updated_approvers),
    ).with_columns(
        pl.col("approval_time").cast(pl.Datetime),
    )

    pulls_df = pulls_df.with_columns(
        pl.when(pl.col("merged_at").is_not_null())
        .then(
            (
                (pl.col("merged_at").cast(pl.Int64) - pl.col("created_at").cast(pl.Int64))
                / 1_000_000_000.0
                / 86400.0
            )
        )
        .otherwise(None)
        .alias("time_to_merge_days"),
        pl.when(pl.col("approval_time").is_not_null())
        .then(
            (
                (pl.col("approval_time").cast(pl.Int64) - pl.col("created_at").cast(pl.Int64))
                / 1_000_000_000.0
                / 86400.0
            )
        )
        .otherwise(None)
        .alias("time_to_approval_days"),
    )

    pr_comments_df = _comments_to_df(comments_accum)
    if pr_comments_df.is_empty():
        pulls_df = pulls_df.with_columns(
            pl.lit(None).alias("time_to_first_non_bot_comment_days"),
            pl.lit(None).alias("first_non_bot_comment_user"),
        )
        return pulls_df, pl.DataFrame()

    pr_comments_df = pr_comments_df.with_columns(
        pl.Series(name="repo", values=[c["repo"] for c in comments_accum]),
        pl.Series(name="pull_number", values=[c["pull_number"] for c in comments_accum]),
    )

    non_bot_comments = pr_comments_df.filter(~pl.col("is_bot"))
    if non_bot_comments.is_empty():
        pulls_df = pulls_df.with_columns(
            pl.lit(None).alias("time_to_first_non_bot_comment_days"),
            pl.lit(None).alias("first_non_bot_comment_user"),
        )
        return pulls_df, pr_comments_df

    non_bot_comments = non_bot_comments.sort("created_at")

    grouped_first_comments = non_bot_comments.group_by(["repo", "pull_number"]).agg(
        first_non_bot_comment_user=pl.col("user").first(),
        first_non_bot_comment_ts=pl.col("created_at").first(),
    )

    pulls_df = pulls_df.join(
        grouped_first_comments,
        left_on=["repo", "number"],
        right_on=["repo", "pull_number"],
        how="left",
    ).with_columns(
        pl.when(pl.col("first_non_bot_comment_ts").is_not_null())
        .then(
            (
                (
                    pl.col("first_non_bot_comment_ts").cast(pl.Int64)
                    - pl.col("created_at").cast(pl.Int64)
                )
                / 1_000_000_000.0
                / 86400.0
            )
        )
        .otherwise(None)
        .alias("time_to_first_non_bot_comment_days")
    )

    return pulls_df, pr_comments_df


def pull_github_data(
    token: str,
    owner: str,
    repos: List[str],
    since: Optional[str] = None,
) -> GitHubData:
    """
    Main entry point: fetch commits, issues, pulls, and releases for the specified
    GitHub repos, compute derived metrics for pull requests, and return the data
    in polars DataFrames.
    """
    fetcher = GitHubFetcher(token, owner, repos)

    def fetch_repo_func(repo_name: str) -> Dict[str, List[dict]]:
        return {
            "commits": fetcher.fetch_commits(repo_name, since=since),
            "issues": fetcher.fetch_issues(repo_name, since=since),
            "pulls": fetcher.fetch_pulls(repo_name, since=since),
            "releases": fetcher.fetch_releases(repo_name),
        }

    concurrency_targets = {r: r for r in repos}
    concurrency_results = run_concurrently(
        fetch_repo_func, targets=concurrency_targets, max_workers=8
    )

    commits_df = pl.DataFrame()
    issues_df = pl.DataFrame()
    pulls_df = pl.DataFrame()
    releases_df = pl.DataFrame()

    for repo_name, data_dict in concurrency_results.items():
        ctemp = (
            pl.DataFrame(data_dict["commits"], infer_schema_length=1000)
            if data_dict["commits"]
            else pl.DataFrame()
        )
        itemp = (
            pl.DataFrame(data_dict["issues"], infer_schema_length=1000)
            if data_dict["issues"]
            else pl.DataFrame()
        )
        ptemp = (
            pl.DataFrame(data_dict["pulls"], infer_schema_length=1000)
            if data_dict["pulls"]
            else pl.DataFrame()
        )
        rtemp = (
            pl.DataFrame(data_dict["releases"], infer_schema_length=1000)
            if data_dict["releases"]
            else pl.DataFrame()
        )

        if not ctemp.is_empty():
            ctemp = ctemp.with_columns(pl.lit(repo_name).alias("repo"))
        if not itemp.is_empty():
            itemp = itemp.with_columns(pl.lit(repo_name).alias("repo"))
        if not ptemp.is_empty():
            ptemp = ptemp.with_columns(pl.lit(repo_name).alias("repo"))
        if not rtemp.is_empty():
            rtemp = rtemp.with_columns(pl.lit(repo_name).alias("repo"))

        commits_df = pl.concat([commits_df, ctemp], how="vertical")
        issues_df = pl.concat([issues_df, itemp], how="vertical")
        pulls_df = pl.concat([pulls_df, ptemp], how="vertical")
        releases_df = pl.concat([releases_df, rtemp], how="vertical")

    processed_pulls_df, pr_comments_df = _process_pull_details(pulls_df, fetcher, since=since)

    return GitHubData(
        commits=commits_df,
        issues=issues_df,
        pulls=processed_pulls_df,
        releases=releases_df,
        pr_comments=pr_comments_df,
    )


def pull_and_write_platforms_github_metrics(
    token: str,
    owner: str,
    repos: List[str],
    since: Optional[str] = None,
):
    """
    Pull GitHub data for the specified repos
    """
    github_data = pull_github_data(token=token, owner=owner, repos=repos, since=since)

    if not github_data.commits.is_empty():
        GitHubPlatform.COMMITS.write(
            dataframe=github_data.commits,
            sort_by=["repo", "commit"],
        )

    if not github_data.issues.is_empty():
        GitHubPlatform.ISSUES.write(
            dataframe=github_data.issues,
            sort_by=["repo", "number"],
        )

    if not github_data.pulls.is_empty():
        GitHubPlatform.PULLS.write(
            dataframe=github_data.pulls,
            sort_by=["repo", "number"],
        )

    if not github_data.releases.is_empty():
        GitHubPlatform.RELEASES.write(
            dataframe=github_data.releases,
            sort_by=["repo", "id"],
        )

    if not github_data.pr_comments.is_empty():
        GitHubPlatform.PR_COMMENTS.write(
            dataframe=github_data.pr_comments,
            sort_by=["repo", "pull_number"],
        )

    log.info("[pull_and_write_platforms_github_metrics] done", repos=repos, since=since)
