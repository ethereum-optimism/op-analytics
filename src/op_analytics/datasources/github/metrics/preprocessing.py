import polars as pl
from op_analytics.coreutils.time import parse_isoformat
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


def compute_earliest_approval(reviews_df: pl.DataFrame) -> pl.DataFrame:
    """Return earliest approval timestamp per PR."""
    approved_reviews = reviews_df.filter(pl.col("state").str.to_uppercase() == "APPROVED")
    return approved_reviews.group_by(["repo", "pr_number"]).agg(
        pl.col("submitted_at").min().alias("approved_at")
    )


def compute_earliest_non_bot_comment(comments_df: pl.DataFrame) -> pl.DataFrame:
    """Return earliest non-bot comment timestamp per PR."""
    non_bot_comments = comments_df.filter(
        pl.col("user").struct.field("login").str.contains(r"(?i)\[bot\]$").not_()
    )
    return non_bot_comments.group_by(["repo", "pr_number"]).agg(
        pl.col("created_at").min().alias("earliest_non_bot_comment_at")
    )


def prepare_prs(
    prs_df: pl.DataFrame, comments_df: pl.DataFrame, reviews_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Prepare PR dataframe by adding derived features and cleaning data.
    """
    prs_df = prs_df.rename({"number": "pr_number"})
    prs_df = deduplicate_prs(prs_df)

    # Convert timestamps
    prs_df = prs_df.with_columns(
        [
            pl.col("created_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime("us"))
            .alias("created_at"),
            pl.col("merged_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime("us"))
            .alias("merged_at"),
            pl.col("closed_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime("us"))
            .alias("closed_at"),
            pl.col("updated_at")
            .map_elements(parse_isoformat, return_dtype=pl.Datetime("us"))
            .alias("updated_at"),
        ]
    )

    # Add derived fields from comments and reviews
    earliest_approval = compute_earliest_approval(reviews_df)
    earliest_non_bot_comment = compute_earliest_non_bot_comment(comments_df)

    prs_df = prs_df.join(earliest_approval, on=["repo", "pr_number"], how="left")
    prs_df = prs_df.join(earliest_non_bot_comment, on=["repo", "pr_number"], how="left")

    # Add reviewer features first
    prs_df = prs_df.with_columns(
        [
            (
                pl.col("requested_reviewers")
                .map_elements(
                    lambda x: len(x) if isinstance(x, list) else 0, return_dtype=pl.UInt32
                )
                .alias("requested_reviewers_count")
            ),
        ]
    )

    # Then add derived features that depend on reviewer features
    prs_df = prs_df.with_columns(
        [
            (pl.col("requested_reviewers_count") > 0).alias("has_requested_reviewers"),
            # Time-based features
            (
                (pl.col("approved_at") - pl.col("created_at"))
                .dt.total_hours()
                .alias("time_to_first_review_hours")
            ),
            (
                (pl.col("earliest_non_bot_comment_at") - pl.col("created_at"))
                .dt.total_hours()
                .alias("time_to_first_non_bot_comment_hours")
            ),
            (
                (pl.col("merged_at") - pl.col("created_at"))
                .dt.total_hours()
                .alias("time_to_merge_hours")
            ),
        ]
    )

    return prs_df


def prepare_comments(comments_df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicate and convert timestamps for comments.
    """
    comments_df = deduplicate_comments(comments_df)
    comments_df = comments_df.with_columns(
        pl.col("created_at")
        .map_elements(parse_isoformat, return_dtype=pl.Datetime("us"))
        .alias("created_at")
    )
    return comments_df


def prepare_reviews(reviews_df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicate and convert timestamps for reviews.
    """
    reviews_df = deduplicate_reviews(reviews_df)
    reviews_df = reviews_df.with_columns(
        pl.col("submitted_at")
        .map_elements(parse_isoformat, return_dtype=pl.Datetime("us"))
        .alias("submitted_at")
    )
    return reviews_df


def deduplicate_prs(prs_df: pl.DataFrame) -> pl.DataFrame:
    if "updated_at" not in prs_df.columns:
        return prs_df
    sorted_df = prs_df.sort(["repo", "pr_number", "updated_at"])
    return sorted_df.unique(subset=["repo", "pr_number"], keep="last")


def deduplicate_comments(comments_df: pl.DataFrame) -> pl.DataFrame:
    if "updated_at" not in comments_df.columns:
        return comments_df
    sorted_df = comments_df.sort(["repo", "pr_number", "id", "updated_at"])
    return sorted_df.unique(subset=["repo", "pr_number", "id"], keep="last")


def deduplicate_reviews(reviews_df: pl.DataFrame) -> pl.DataFrame:
    if "submitted_at" not in reviews_df.columns:
        return reviews_df
    sorted_df = reviews_df.sort(["repo", "pr_number", "id", "submitted_at"])
    return sorted_df.unique(subset=["repo", "pr_number", "id"], keep="last")
