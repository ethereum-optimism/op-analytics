import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import parse_isoformat
from op_analytics.coreutils.misc import raise_for_schema_mismatch

log = structlog.get_logger()

# Define expected schema as a constant
EXPECTED_DAILY_METRICS_SCHEMA = {
    "repo": pl.Utf8,
    "number_of_prs": pl.UInt32,
    "avg_time_to_approval_days": pl.Float64,
    "avg_time_to_first_non_bot_comment_days": pl.Float64,
    "avg_time_to_merge_days": pl.Float64,
    "approval_ratio": pl.Float64,
    "avg_comments_per_pr": pl.Float64,
    "merged_ratio": pl.Float64,
    "active_contributors": pl.UInt32,
}


def compute_pr_metrics(
    prs_df: pl.DataFrame,
    comments_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute GitHub PR metrics

    Uses the following input tables:
    - PRs (Github.PRS)
    - Comments (Github.PR_COMMENTS)
    - Reviews (Github.PR_REVIEWS)

    Computes daily PR metrics for each (repo, dt) partition:
    - number_of_prs
    - avg_time_to_approval_days
    - avg_time_to_first_non_bot_comment_days
    - avg_time_to_merge_days
    - approval_ratio       (# PRs with an approval vs total)
    - avg_comments_per_pr
    - merged_ratio        (# PRs merged vs total)
    - active_contributors (unique logins who opened PRs on that date)

    Args:
        prs_df: DataFrame containing PR data from Github.PRS
        comments_df: DataFrame containing PR comments from Github.PR_COMMENTS
        reviews_df: DataFrame containing PR reviews from Github.PR_REVIEWS

    Returns:
        pl.DataFrame: Daily metrics aggregated by repo and date
    """
    # Convert timestamps to datetime
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

    reviews_df = reviews_df.with_columns(
        [
            pl.col("submitted_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime)
            .alias("submitted_at")
        ]
    )

    # Compute earliest approval from reviews (where state = "APPROVED")
    # For each (repo, pr_number): earliest "submitted_at" => "approved_at"
    approved_reviews = reviews_df.filter(pl.col("state").str.to_uppercase() == "APPROVED")
    earliest_approval = approved_reviews.group_by(["repo", "pr_number"]).agg(
        pl.col("submitted_at").min().alias("approved_at")
    )

    # Join earliest approval into PR data, matching pr_number -> number
    enriched_prs = prs_df.join(
        earliest_approval.rename({"pr_number": "number"}),
        on=["repo", "number"],
        how="left",
    )

    # Compute earliest non-bot comment
    # Filter out comments whose user.login ends with [bot]
    non_bot_comments = comments_df.filter(
        pl.col("user").struct.field("login").str.contains(r"(?i)\[bot\]$").not_()
    )

    earliest_non_bot_comment = non_bot_comments.group_by(["repo", "pr_number"]).agg(
        pl.col("created_at").min().alias("earliest_non_bot_comment_at")
    )

    # Join earliest non-bot comment into PR data
    enriched_prs = enriched_prs.join(
        earliest_non_bot_comment.rename({"pr_number": "number"}),
        on=["repo", "number"],
        how="left",
    )

    # Compute intervals from PR creation date
    # - time_to_approval_days  = (approved_at - created_at).days
    # - time_to_first_non_bot_comment_days
    # - time_to_merge_days
    enriched_prs = enriched_prs.with_columns(
        [
            ((pl.col("approved_at") - pl.col("created_at")).dt.total_days()).alias(
                "time_to_approval_days"
            ),
            ((pl.col("earliest_non_bot_comment_at") - pl.col("created_at")).dt.total_days()).alias(
                "time_to_first_non_bot_comment_days"
            ),
            ((pl.col("merged_at") - pl.col("created_at")).dt.total_days()).alias(
                "time_to_merge_days"
            ),
        ]
    )

    # Booleans for 'is_approved', 'is_merged'
    enriched_prs = enriched_prs.with_columns(
        [
            pl.col("approved_at").is_not_null().alias("is_approved"),
            pl.col("merged_at").is_not_null().alias("is_merged"),
        ]
    )

    # Count comments per PR
    # For each (repo, pr_number) => total # of comments
    comment_counts = comments_df.group_by(["repo", "pr_number"]).agg(
        pl.count().alias("comment_count")
    )

    enriched_prs = enriched_prs.join(
        comment_counts.rename({"pr_number": "number"}),
        on=["repo", "number"],
        how="left",
    ).with_columns(pl.col("comment_count").fill_null(0))

    # Aggregate daily metrics at (repo, dt)
    daily_metrics = (
        enriched_prs.group_by("repo")
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
        .sort("repo")
    )

    # Validate schema before returning
    raise_for_schema_mismatch(
        actual_schema=daily_metrics.schema,
        expected_schema=EXPECTED_DAILY_METRICS_SCHEMA,
    )

    return daily_metrics
