import polars as pl

from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


def compute_pr_metrics(
    prs_df: pl.DataFrame,
    comments_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Compute daily GitHub PR metrics per repository.

    Args:
        prs_df: DataFrame with columns:
            [
                repo, number, created_at, merged_at, user, state, draft,
                (others like html_url, labels, etc.)
            ]
            NOTE: user is a struct with fields {login: str, id: int}
        comments_df: DataFrame with columns:
            [repo, pr_number, created_at, user, ...]
            NOTE: user is a struct with fields {login: str, id: int}
        reviews_df: DataFrame with columns:
            [repo, pr_number, created_at, user, author_association, ...]
            NOTE: user is a struct with fields {login: str, id: int}

    Returns:
        DataFrame with columns:
            - repo: str
            - dt: pl.Date
            - number_of_prs: int
            - avg_time_to_approval_days: float
            - avg_time_to_first_non_bot_comment_days: float
            - avg_time_to_merge_days: float
            - approval_ratio: float (0-1)
            - avg_comments_per_pr: float
            - merged_ratio: float (0-1)
            - active_contributors: int (unique PR creators per day)
    """

    # Input validation
    required_pr_cols = {"repo", "number", "created_at", "merged_at", "user", "state", "draft"}
    required_comment_cols = {"repo", "pr_number", "created_at", "user"}
    required_review_cols = {"repo", "pr_number", "created_at", "user", "author_association"}

    if not all(col in prs_df.columns for col in required_pr_cols):
        raise ValueError(f"PRs DataFrame missing required columns: {required_pr_cols}")
    if not all(col in comments_df.columns for col in required_comment_cols):
        raise ValueError(f"Comments DataFrame missing required columns: {required_comment_cols}")
    if not all(col in reviews_df.columns for col in required_review_cols):
        raise ValueError(f"Reviews DataFrame missing required columns: {required_review_cols}")

    # Convert string timestamps to datetime in each DF.
    # Note: If your Polars version doesn't support `strict=False`,
    #       remove it or handle unmatched formats as needed.
    prs_df = prs_df.with_columns(
        [
            pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False),
            pl.col("merged_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False),
        ]
    )

    comments_df = comments_df.with_columns(
        [
            pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False),
        ]
    )

    reviews_df = reviews_df.with_columns(
        [
            pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False),
        ]
    )

    # Add dt column for daily grouping
    prs_df = prs_df.with_columns(pl.col("created_at").cast(pl.Date).alias("dt"))

    # 1. Compute earliest approval per PR
    approved_reviews = (
        reviews_df.filter(pl.col("author_association") == "MEMBER")
        .group_by(["repo", "pr_number"])
        .agg(pl.col("created_at").min().alias("earliest_approval_at"))
    )

    # 2. Compute earliest non-bot comment per PR
    earliest_non_bot_comment = (
        comments_df.filter(~pl.col("user").struct.field("login").str.contains(r"(?i)\[bot\]$"))
        .group_by(["repo", "pr_number"])
        .agg(pl.col("created_at").min().alias("earliest_comment_at"))
    )

    # 3. Count total comments per PR
    comments_per_pr = comments_df.group_by(["repo", "pr_number"]).agg(
        pl.count().alias("comment_count")
    )

    # 4. Join everything to PRs
    enriched_prs = (
        prs_df.join(
            approved_reviews, left_on=["repo", "number"], right_on=["repo", "pr_number"], how="left"
        )
        .join(
            earliest_non_bot_comment,
            left_on=["repo", "number"],
            right_on=["repo", "pr_number"],
            how="left",
        )
        .join(
            comments_per_pr, left_on=["repo", "number"], right_on=["repo", "pr_number"], how="left"
        )
        .with_columns(
            [
                pl.col("earliest_approval_at").is_not_null().alias("is_approved"),
                pl.col("merged_at").is_not_null().alias("is_merged"),
                pl.col("comment_count").fill_null(0),
            ]
        )
    )

    # Helper for computing time differences in days
    def days_between(end_col: str, start_col: str, alias_name: str) -> pl.Expr:
        """
        Subtract two Datetime columns (end_col - start_col) and convert to days.
        Polars stores Datetime in microseconds, so we convert accordingly.
        """
        return (
            (pl.col(end_col).cast(pl.Int64) - pl.col(start_col).cast(pl.Int64))
            / (24 * 60 * 60 * 1_000_000)
        ).alias(alias_name)

    # 5. Compute distinct time differences
    enriched_prs = enriched_prs.with_columns(
        [
            days_between("earliest_approval_at", "created_at", "time_to_approval_days"),
            days_between("earliest_comment_at", "created_at", "time_to_first_non_bot_comment_days"),
            days_between("merged_at", "created_at", "time_to_merge_days"),
        ]
    )

    # 6. Aggregate daily metrics
    daily_metrics = (
        enriched_prs.group_by(["repo", "dt"])
        .agg(
            [
                pl.count().alias("number_of_prs"),
                pl.col("time_to_approval_days").mean().alias("avg_time_to_approval_days"),
                pl.col("time_to_first_non_bot_comment_days")
                .mean()
                .alias("avg_time_to_first_non_bot_comment_days"),
                pl.col("time_to_merge_days").mean().alias("avg_time_to_merge_days"),
                (pl.col("is_approved").sum() / pl.count()).alias("approval_ratio"),
                (pl.col("comment_count").sum() / pl.count()).alias("avg_comments_per_pr"),
                (pl.col("is_merged").sum() / pl.count()).alias("merged_ratio"),
                pl.col("user").struct.field("login").n_unique().alias("active_contributors"),
            ]
        )
        .sort(["repo", "dt"])
    )

    log.info(
        "computed daily PR metrics",
        repos=daily_metrics["repo"].unique().to_list(),
        date_range=[
            daily_metrics["dt"].min().strftime("%Y-%m-%d") if daily_metrics.height > 0 else None,
            daily_metrics["dt"].max().strftime("%Y-%m-%d") if daily_metrics.height > 0 else None,
        ],
    )

    return daily_metrics
