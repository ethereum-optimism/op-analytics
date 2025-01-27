import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import parse_isoformat

log = structlog.get_logger()


def compute_pr_metrics(
    prs_df: pl.DataFrame,
    comments_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Computes daily GitHub PR metrics, returning one row per (repo, dt) with columns:
      - number_of_prs
      - avg_time_to_approval_days
      - avg_time_to_first_non_bot_comment_days
      - avg_time_to_merge_days
      - approval_ratio
      - avg_comments_per_pr
      - merged_ratio
      - active_contributors

    Args:
        prs_df (pl.DataFrame): Polars DataFrame of PRs with columns roughly:
            [
                "html_url", "number", "created_at", "merged_at", "approved_at",
                "user", "repo", "dt", ...
            ]
            * "created_at", "merged_at", and (optionally) "approved_at"
              might be ISO-format strings, e.g. "2024-05-01T12:34:56Z"
            * "dt" is a Polars Date for daily grouping.

        comments_df (pl.DataFrame): Polars DataFrame of PR comments, with columns:
            [
                "pr_number", "created_at", "user", "repo", "dt", ...
            ]
            * "created_at" might be an ISO-format string, e.g. "2024-05-01T12:34:56Z"

    Returns:
        pl.DataFrame with columns:
            [
              "repo",
              "dt",
              "number_of_prs",
              "avg_time_to_approval_days",
              "avg_time_to_first_non_bot_comment_days",
              "avg_time_to_merge_days",
              "approval_ratio",
              "avg_comments_per_pr",
              "merged_ratio",
              "active_contributors",
            ]
        Sorted by "repo" then "dt".
    """

    prs_df = prs_df.with_columns(
        [
            pl.col("created_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("created_at"),
            pl.col("merged_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("merged_at"),
        ]
    )

    comments_df = comments_df.with_columns(
        [
            pl.col("created_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("created_at")
        ]
    )

    earliest_non_bot_comment = (
        comments_df.filter(
            pl.col("user").struct.field("login").str.contains(r"(?i)\[bot\]$").not_()
        )
        .group_by(["repo", "pr_number"])
        .agg(pl.col("created_at").min().alias("earliest_non_bot_comment_at"))
    )

    comments_per_pr = comments_df.group_by(["repo", "pr_number"]).agg(
        pl.len().alias("comment_count")
    )

    # Join earliest_comment & comment_count to the PRs
    enriched_prs = (
        prs_df.join(
            earliest_non_bot_comment.rename({"pr_number": "number"}),
            on=["repo", "number"],
            how="left",
        )
        .join(
            comments_per_pr.rename({"pr_number": "number"}),
            on=["repo", "number"],
            how="left",
        )
        .with_columns(
            [
                pl.col("approved_at").is_not_null().alias("is_approved"),
                pl.col("merged_at").is_not_null().alias("is_merged"),
                pl.col("comment_count").fill_null(0),
            ]
        )
    )
    # Calculate time differences in days between datetime columns
    time_exprs = [
        ((pl.col("earliest_non_bot_comment_at") - pl.col("created_at")).dt.days()).alias(
            "time_to_first_non_bot_comment_days"
        ),
        ((pl.col("merged_at") - pl.col("created_at")).dt.days()).alias("time_to_merge_days"),
        ((pl.col("approved_at") - pl.col("created_at")).dt.days()).alias("time_to_approval_days"),
    ]

    enriched_prs = enriched_prs.with_columns(time_exprs)

    daily_metrics = (
        enriched_prs.group_by(["repo", "dt"])
        .agg(
            [
                pl.len().alias("number_of_prs"),
                pl.col("time_to_approval_days").mean().alias("avg_time_to_approval_days"),
                pl.col("time_to_first_non_bot_comment_days")
                .mean()
                .alias("avg_time_to_first_non_bot_comment_days"),
                pl.col("time_to_merge_days").mean().alias("avg_time_to_merge_days"),
                (pl.col("is_approved").sum() / pl.len()).alias("approval_ratio"),
                (pl.col("comment_count").sum() / pl.len()).alias("avg_comments_per_pr"),
                (pl.col("is_merged").sum() / pl.len()).alias("merged_ratio"),
                pl.col("user").struct.field("login").n_unique().alias("active_contributors"),
            ]
        )
        .sort(["repo", "dt"])
    )

    return daily_metrics
