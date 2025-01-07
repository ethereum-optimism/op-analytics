import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
import polars as pl

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


# --- Rate Limit Helpers --- #


def get_rate_limit_info(session: requests.Session, token: Optional[str] = None) -> dict:
    """
    Get the current GitHub rate limit for the 'core' resource.
    Returns dict with limit, remaining, and reset (epoch) values.
    """
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    resp = session.get("https://api.github.com/rate_limit", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    core = data["resources"]["core"]
    return {
        "limit": core["limit"],
        "remaining": core["remaining"],
        "reset": core["reset"],  # integer epoch
    }


def wait_for_rate_limit(session: requests.Session, token: Optional[str] = None) -> None:
    """
    If rate limit is exhausted, wait until reset time.
    """
    while True:
        info = get_rate_limit_info(session, token=token)
        remaining = info["remaining"]
        reset_epoch = info["reset"]
        log.info(
            "GitHub Rate Limit",
            current_utc=str(now()),
            remaining=remaining,
            reset_epoch=reset_epoch,
        )

        if remaining == 0:
            wait_until = datetime_fromepoch(reset_epoch)
            sleep_seconds = (wait_until - now()).total_seconds()
            if sleep_seconds > 0:
                log.warning(
                    "Rate limit exhausted, waiting.",
                    wait_time_sec=sleep_seconds,
                    reset_time=str(wait_until),
                )
                time.sleep(sleep_seconds + 2)
            else:
                log.info("Rate limit presumably just reset, proceeding.")
        else:
            break


# --- Data Container --- #


@dataclass
class GitHubData:
    """
    Holds the data frames fetched from GitHub.
    """

    commits: pl.DataFrame
    issues: pl.DataFrame
    pulls: pl.DataFrame
    releases: pl.DataFrame
    pr_comments: pl.DataFrame


# --- Main GitHub Fetcher Class --- #


class GitHubFetcher:
    """
    Fetch various data (commits, issues, etc.) from GitHub, handling pagination and rate limits.
    """

    def __init__(self, token: str, owner: str, repos: List[str]):
        self.token = token
        self.owner = owner
        self.repos = repos
        self.session = new_session()
        self.session.headers.update({"Authorization": f"token {self.token}"})

    def _fetch_single_page(self, url: str) -> list | dict:
        """
        Fetch one page with retry and exponential backoff.
        """
        max_attempts = 5
        attempt = 0
        backoff = 5

        while True:
            attempt += 1
            try:
                wait_for_rate_limit(self.session, token=self.token)
                return get_data(session=self.session, url=url, retry_attempts=1)
            except Exception as exc:
                log.warning("Error fetching page", url=url, attempt=attempt, error=str(exc))
                if attempt >= max_attempts:
                    log.error("Exceeded max attempts, re-raising.", url=url)
                    raise
                time.sleep(backoff)
                backoff *= 2

    def _fetch_all_pages(self, first_url: str) -> List[dict]:
        """
        Fetch all pages for a given endpoint by:
        1) Fetching the first page.
        2) Checking 'Link' headers for next/last pages.
        3) Fetching subsequent pages concurrently.
        """
        first_data = self._fetch_single_page(first_url)
        first_data = first_data if isinstance(first_data, list) else [first_data]
        all_items = first_data.copy()

        head_resp = self.session.head(first_url)
        link_header = head_resp.headers.get("Link")
        if not link_header:
            return all_items

        # Check for 'rel="last"' to find the last page link
        last_link = None
        for segment in link_header.split(","):
            if 'rel="last"' in segment:
                start = segment.find("<") + 1
                end = segment.find(">")
                last_link = segment[start:end]
                break

        # If no "last" link, just see if there's a "next" link
        if not last_link:
            next_link = None
            for segment in link_header.split(","):
                if 'rel="next"' in segment:
                    start = segment.find("<") + 1
                    end = segment.find(">")
                    next_link = segment[start:end]
                    break
            if next_link:
                extra = self._fetch_single_page(next_link)
                extra = extra if isinstance(extra, list) else [extra]
                all_items.extend(extra)
            return all_items

        from urllib.parse import parse_qs, urlparse

        parsed_last = urlparse(last_link)
        qs_last = parse_qs(parsed_last.query)
        last_page = int(qs_last.get("page", [1])[0])
        if last_page <= 1:
            return all_items

        parsed_init = urlparse(first_url)
        qs_init = parse_qs(parsed_init.query)
        qs_init.pop("page", None)  # remove existing page param

        def build_page_url(page_num: int) -> str:
            from urllib.parse import urlencode

            new_qs = qs_init.copy()
            new_qs["page"] = str(page_num)
            return f"{parsed_init.scheme}://{parsed_init.netloc}{parsed_init.path}?{urlencode(new_qs, doseq=True)}"

        page_urls = [build_page_url(p) for p in range(2, last_page + 1)]

        def fetch_func(u: str) -> List[dict]:
            result = self._fetch_single_page(u)
            return result if isinstance(result, list) else [result]

        targets = {f"page_{i}": url for i, url in enumerate(page_urls, start=2)}
        results = run_concurrently(fetch_func, targets=targets, max_workers=4)

        for _, data in results.items():
            all_items.extend(data)
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
        url = f"https://api.github.com/repos/{self.owner}/{repo}/pulls?state=all&sort=updated&direction=desc"
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


# --- Pull Request Details Processing --- #


def _get_first_approval_info(reviews: List[dict]) -> Tuple[pl.Datetime, Optional[str]]:
    """
    Find the first 'APPROVED' review's time and user.
    Returns (approval_datetime, reviewer_login).
    """
    approved = next((r for r in reviews if r.get("state", "").upper() == "APPROVED"), None)
    if approved:
        submitted_at = parse_isoformat(approved["submitted_at"].replace("Z", "+00:00"))
        return submitted_at, approved["user"]["login"] if approved.get("user") else None
    return datetime_fromepoch(0), None


def _comments_list_to_df(comments: List[dict]) -> pl.DataFrame:
    """
    Convert comment dicts to a Polars DataFrame.
    """
    if not comments:
        return pl.DataFrame(
            schema={"user": pl.Utf8, "created_at": pl.Datetime, "is_bot": pl.Boolean}
        )

    df = pl.DataFrame(
        {
            "user": [c["user"].get("login") if c.get("user") else None for c in comments],
            "created_at": [c["created_at"] for c in comments],
        }
    ).with_columns(
        pl.col("created_at")
        .str.replace("Z$", "+00:00")
        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
        pl.col("user").fill_null(""),
    )
    return df.with_columns(pl.col("user").str.contains(r"(?i)\[bot\]").alias("is_bot"))


def _process_pull_details(
    pulls_df: pl.DataFrame, fetcher: GitHubFetcher, since: Optional[str] = None
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Fetch reviews/comments for each PR, then calculate time-to-merge, time-to-approval, etc.
    """
    if pulls_df.is_empty():
        return pulls_df, pl.DataFrame()

    # Ensure we have these columns
    for col in ["created_at", "merged_at"]:
        if col not in pulls_df.columns:
            pulls_df = pulls_df.with_columns(pl.lit(None).alias(col))

    # Convert string times to datetime
    pulls_df = (
        pulls_df.with_columns(pl.col("created_at").cast(pl.Utf8).fill_null(""))
        .with_columns(pl.col("merged_at").cast(pl.Utf8).fill_null(""))
        .with_columns(
            pl.col("created_at")
            .str.replace("Z$", "+00:00")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
            pl.col("merged_at")
            .str.replace("Z$", "+00:00")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
        )
    )

    # Filter by 'since' if provided
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
        repo = pr_info.get("repo")
        number = pr_info.get("number")
        idx = pr_info.get("_idx", -1)
        if number is None:
            return {"idx": idx, "approval_time": None, "approver": None, "comments": []}

        reviews = fetcher.fetch_pr_reviews(repo, number)
        approval_dt, approver = _get_first_approval_info(reviews)
        raw_comments = fetcher.fetch_pr_comments(repo, number)

        return {
            "idx": idx,
            "approval_time": None if approval_dt == datetime_fromepoch(0) else approval_dt,
            "approver": approver,
            "comments": raw_comments,
        }

    results = run_concurrently(fetch_pr_data, targets, max_workers=8)

    # Populate arrays for new columns
    updated_approvals = [None] * len(targets)
    updated_approvers = [None] * len(targets)
    comments_accum: List[dict] = []

    for key, data in results.items():
        i = int(key.replace("pr_", ""))
        updated_approvals[i] = data["approval_time"]
        updated_approvers[i] = data["approver"]

        original = targets[key]
        for c in data["comments"] or []:
            c["repo"] = original.get("repo")
            c["pull_number"] = original.get("number")
        comments_accum.extend(data["comments"] or [])

    # Merge new columns into the main DF
    pulls_df = pulls_df.with_columns(
        pl.Series("approval_time", updated_approvals).cast(pl.Datetime),
        pl.Series("approver", updated_approvers),
    )

    # Calculate time-to-merge and time-to-approval in days
    pulls_df = pulls_df.with_columns(
        pl.when(pl.col("merged_at").is_not_null())
        .then(
            (
                (pl.col("merged_at").cast(pl.Int64) - pl.col("created_at").cast(pl.Int64))
                / 1_000_000_000
                / 86400
            )
        )
        .otherwise(None)
        .alias("time_to_merge_days"),
        pl.when(pl.col("approval_time").is_not_null())
        .then(
            (
                (pl.col("approval_time").cast(pl.Int64) - pl.col("created_at").cast(pl.Int64))
                / 1_000_000_000
                / 86400
            )
        )
        .otherwise(None)
        .alias("time_to_approval_days"),
    )

    # Convert comments to DF
    pr_comments_df = _comments_list_to_df(comments_accum)
    if pr_comments_df.is_empty():
        pulls_df = pulls_df.with_columns(
            pl.lit(None).alias("time_to_first_non_bot_comment_days"),
            pl.lit(None).alias("first_non_bot_comment_user"),
        )
        return pulls_df, pl.DataFrame()

    pr_comments_df = pr_comments_df.with_columns(
        pl.Series("repo", [c["repo"] for c in comments_accum]),
        pl.Series("pull_number", [c["pull_number"] for c in comments_accum]),
    )

    # Grab the earliest non-bot comment per PR
    non_bot_comments = pr_comments_df.filter(~pl.col("is_bot")).sort("created_at")
    if non_bot_comments.is_empty():
        pulls_df = pulls_df.with_columns(
            pl.lit(None).alias("time_to_first_non_bot_comment_days"),
            pl.lit(None).alias("first_non_bot_comment_user"),
        )
        return pulls_df, pr_comments_df

    grouped = non_bot_comments.group_by(["repo", "pull_number"]).agg(
        first_non_bot_comment_user=pl.col("user").first(),
        first_non_bot_comment_ts=pl.col("created_at").first(),
    )

    pulls_df = pulls_df.join(
        grouped, left_on=["repo", "number"], right_on=["repo", "pull_number"], how="left"
    ).with_columns(
        pl.when(pl.col("first_non_bot_comment_ts").is_not_null())
        .then(
            (
                (
                    pl.col("first_non_bot_comment_ts").cast(pl.Int64)
                    - pl.col("created_at").cast(pl.Int64)
                )
                / 1_000_000_000
                / 86400
            )
        )
        .otherwise(None)
        .alias("time_to_first_non_bot_comment_days")
    )

    return pulls_df, pr_comments_df


# --- Main Entry Points --- #


def pull_github_data(
    token: str, owner: str, repos: List[str], since: Optional[str] = None
) -> GitHubData:
    """
    Fetch commits, issues, pulls, and releases, then compute extra pull request metrics.
    """
    fetcher = GitHubFetcher(token, owner, repos)

    def fetch_repo_data(repo_name: str) -> Dict[str, List[dict]]:
        return {
            "commits": fetcher.fetch_commits(repo_name, since=since),
            "issues": fetcher.fetch_issues(repo_name, since=since),
            "pulls": fetcher.fetch_pulls(repo_name, since=since),
            "releases": fetcher.fetch_releases(repo_name),
        }

    targets = {r: r for r in repos}
    results = run_concurrently(fetch_repo_data, targets=targets, max_workers=8)

    commits_df = pl.DataFrame()
    issues_df = pl.DataFrame()
    pulls_df = pl.DataFrame()
    releases_df = pl.DataFrame()

    for repo_name, data in results.items():
        ctemp = (
            pl.DataFrame(data["commits"], infer_schema_length=1000)
            if data["commits"]
            else pl.DataFrame()
        )
        itemp = (
            pl.DataFrame(data["issues"], infer_schema_length=1000)
            if data["issues"]
            else pl.DataFrame()
        )
        ptemp = (
            pl.DataFrame(data["pulls"], infer_schema_length=1000)
            if data["pulls"]
            else pl.DataFrame()
        )
        rtemp = (
            pl.DataFrame(data["releases"], infer_schema_length=1000)
            if data["releases"]
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

    processed_pulls, pr_comments = _process_pull_details(pulls_df, fetcher, since=since)

    return GitHubData(
        commits=commits_df,
        issues=issues_df,
        pulls=processed_pulls,
        releases=releases_df,
        pr_comments=pr_comments,
    )


def pull_and_write_platforms_github_metrics(
    token: str, owner: str, repos: List[str], since: Optional[str] = None
):
    """
    Fetch GitHub data, then write to GitHubPlatform.
    """
    data = pull_github_data(token, owner, repos, since)

    if not data.commits.is_empty():
        GitHubPlatform.COMMITS.write(
            dataframe=data.commits,
            sort_by=["repo", "commit"],
        )

    if not data.issues.is_empty():
        GitHubPlatform.ISSUES.write(
            dataframe=data.issues,
            sort_by=["repo", "number"],
        )

    if not data.pulls.is_empty():
        GitHubPlatform.PULLS.write(
            dataframe=data.pulls,
            sort_by=["repo", "number"],
        )

    if not data.releases.is_empty():
        GitHubPlatform.RELEASES.write(
            dataframe=data.releases,
            sort_by=["repo", "id"],
        )

    if not data.pr_comments.is_empty():
        GitHubPlatform.PR_COMMENTS.write(
            dataframe=data.pr_comments,
            sort_by=["repo", "pull_number"],
        )

    log.info("Done pulling GitHub metrics", repos=repos, since=since)
