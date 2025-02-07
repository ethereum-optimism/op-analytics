import polars as pl
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


def compute_cohort_metrics(prs_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute cohort metrics based on PR creation date.
    """
    df = (
        prs_df.group_by_dynamic(
            index_column="created_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",  # type: ignore
            include_boundaries=True,
        )
        .agg(
            [
                pl.col("pr_number").n_unique().alias("new_prs"),
                pl.col("time_to_first_review_hours")
                .median()
                .alias("median_time_to_first_review_hours"),
                pl.col("time_to_first_non_bot_comment_hours")
                .median()
                .alias("median_time_to_first_non_bot_comment_hours"),
                pl.col("time_to_merge_hours").median().alias("median_time_to_merge_hours"),
                pl.col("user").struct.field("login").n_unique().alias("unique_contributors"),
                pl.col("has_requested_reviewers")
                .sum()
                .cast(pl.UInt32)
                .alias("review_requested_prs"),
            ]
        )
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
