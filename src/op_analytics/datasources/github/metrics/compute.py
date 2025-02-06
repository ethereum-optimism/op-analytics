import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.time import now
from op_analytics.datasources.github.metrics.preprocessing import (
    prepare_prs,
    prepare_comments,
    prepare_reviews,
)
from op_analytics.datasources.github.metrics import (
    cohort,
    prmerge,
    prclose,
    prupdate,
    prapproval,
    prcomment,
    prreview,
)

log = structlog.get_logger()

METRICS_SCHEMA = {
    "repo": pl.Utf8,
    "period_start": pl.Datetime("us"),
    "period_end": pl.Datetime("us"),
    "new_prs": pl.UInt32,
    "merged_prs": pl.UInt32,
    "closed_prs": pl.UInt32,
    "active_prs": pl.UInt32,
    "median_time_to_first_review_hours": pl.Float64,
    "median_time_to_first_non_bot_comment_hours": pl.Float64,
    "median_time_to_merge_hours": pl.Float64,
    "total_comments": pl.UInt32,
    "total_reviews": pl.UInt32,
    "unique_commenters": pl.UInt32,
    "unique_reviewers": pl.UInt32,
    "unique_contributors": pl.UInt32,
    "approved_prs": pl.UInt32,
    "rejected_prs": pl.UInt32,
    "review_requested_prs": pl.UInt32,
    "period_type": pl.Utf8,
    "approval_ratio": pl.Float64,
    "merge_ratio": pl.Float64,
    "closed_ratio": pl.Float64,
    "comment_intensity": pl.Float64,
    "review_intensity": pl.Float64,
    "active_ratio": pl.Float64,
    "response_time_ratio": pl.Float64,
    "contributor_engagement": pl.Float64,
}


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


def add_derived_fields_to_prs(
    prs_df: pl.DataFrame, comments_df: pl.DataFrame, reviews_df: pl.DataFrame
) -> pl.DataFrame:
    """Join derived fields to the PR dataframe."""
    earliest_approval = compute_earliest_approval(reviews_df)
    earliest_non_bot_comment = compute_earliest_non_bot_comment(comments_df)

    prs_df = prs_df.join(earliest_approval, on=["repo", "pr_number"], how="left")
    prs_df = prs_df.join(earliest_non_bot_comment, on=["repo", "pr_number"], how="left")
    return prs_df


def _compute_rolling_for_interval(
    prs_df: pl.DataFrame,
    comments_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
    period_days: int,
    period_label: str,
) -> pl.DataFrame:
    """Compute metrics for a rolling window of given size"""

    period_str = f"{period_days}d"

    # Compute each metric group using the new modular functions
    cohort_agg = cohort.compute_cohort_metrics(prs_df, period_str)
    merge_agg = prmerge.compute_prmerge_metrics(prs_df, period_str)
    close_agg = prclose.compute_prclose_metrics(prs_df, period_str)
    update_agg = prupdate.compute_prupdate_metrics(prs_df, period_str)
    approve_agg = prapproval.compute_prapproval_metrics(prs_df, period_str)
    comment_agg = prcomment.compute_prcomment_metrics(comments_df, period_str)
    review_agg = prreview.compute_prreview_metrics(reviews_df, period_str)

    # Join all aggregations
    dfs_to_join = [
        merge_agg,
        close_agg,
        update_agg,
        approve_agg,
        comment_agg,
        review_agg,
    ]
    merged = cohort_agg
    for df_ in dfs_to_join:
        merged = merged.join(
            df_,
            how="outer",
            on=["repo", "period_start", "period_end"],
        )

    merged = merged.with_columns(pl.lit(f"rolling_{period_label}").alias("period_type"))

    current_time = now()
    final = merged.filter(pl.col("period_end") <= current_time)

    return final


def compute_all_metrics(
    prs_df: pl.DataFrame,
    comments_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute metrics for multiple rolling window sizes and combine results"""
    # Preprocess dataframes
    prs_df = prepare_prs(prs_df)
    comments_df = prepare_comments(comments_df)
    reviews_df = prepare_reviews(reviews_df)
    # Add derived fields to PRs
    prs_df = add_derived_fields_to_prs(prs_df, comments_df, reviews_df)
    # Compute metrics for each interval
    intervals = {
        "week": 7,
        "month": 30,
        "3months": 90,
        "6months": 180,
        "year": 365,
    }

    # Filter out bot comments and sort data
    comments_df = comments_df.filter(
        pl.col("user").struct.field("login").str.contains(r"(?i)\[bot\]$").not_()
    )
    prs_df = prs_df.sort("created_at")
    comments_df = comments_df.sort("created_at")
    reviews_df = reviews_df.sort("submitted_at")

    frames = []
    for label, days in intervals.items():
        frame = _compute_rolling_for_interval(
            prs_df,
            comments_df,
            reviews_df,
            period_days=days,
            period_label=label,
        )
        frames.append(frame)

    final = pl.concat(frames, how="vertical")

    final = final.with_columns(
        [
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("approved_prs") / pl.col("new_prs"))
            .otherwise(0)
            .alias("approval_ratio"),
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("merged_prs") / pl.col("new_prs"))
            .otherwise(0)
            .alias("merge_ratio"),
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("closed_prs") / pl.col("new_prs"))
            .otherwise(0)
            .alias("closed_ratio"),
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("total_comments") / pl.col("new_prs"))
            .otherwise(0)
            .alias("comment_intensity"),
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("total_reviews") / pl.col("new_prs"))
            .otherwise(0)
            .alias("review_intensity"),
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("active_prs") / pl.col("new_prs"))
            .otherwise(0)
            .alias("active_ratio"),
            pl.when(pl.col("median_time_to_merge_hours") > 0)
            .then(
                pl.col("median_time_to_first_non_bot_comment_hours")
                / pl.col("median_time_to_merge_hours")
            )
            .otherwise(0)
            .alias("response_time_ratio"),
            pl.when(pl.col("new_prs") > 0)
            .then(pl.col("unique_contributors") / pl.col("new_prs"))
            .otherwise(0)
            .alias("contributor_engagement"),
        ]
    )

    final = final.select(METRICS_SCHEMA.keys())

    expected_schema = pl.Schema(METRICS_SCHEMA)
    raise_for_schema_mismatch(final.schema, expected_schema)
    return final
