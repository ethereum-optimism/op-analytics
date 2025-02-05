import polars as pl
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


def compute_cohort_metrics(prs_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute cohort metrics based on PR creation date.
    """
    # Pure python implementation of the requested_reviewers_count column.
    reviewers_col = prs_df["requested_reviewers"].to_list()
    counts = [len(item) if isinstance(item, list) else 0 for item in reviewers_col]
    if "requested_reviewers_count" in prs_df.columns:
        prs_df = prs_df.drop("requested_reviewers_count")

    prs_df = prs_df.with_columns(pl.Series(name="requested_reviewers_count", values=counts))

    df = (
        prs_df.group_by_dynamic(
            index_column="created_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",
            include_boundaries=True,
        )
        .agg(
            [
                pl.col("pr_number").n_unique().alias("new_prs"),
                ((pl.col("approved_at") - pl.col("created_at")).dt.total_hours())
                .median()
                .alias("median_time_to_first_review_hours"),
                ((pl.col("earliest_non_bot_comment_at") - pl.col("created_at")).dt.total_hours())
                .median()
                .alias("median_time_to_first_non_bot_comment_hours"),
                ((pl.col("merged_at") - pl.col("created_at")).dt.total_hours())
                .median()
                .alias("median_time_to_merge_hours"),
                pl.col("user").struct.field("login").n_unique().alias("unique_contributors"),
                # Turn any row with requested_reviewers_count > 0 into 1, then sum them.
                pl.when(pl.col("requested_reviewers_count") > 0)
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
                .sum()
                .cast(pl.UInt32)
                .alias("review_requested_prs"),
            ]
        )
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
