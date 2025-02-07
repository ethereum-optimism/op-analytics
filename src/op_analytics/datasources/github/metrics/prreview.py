import polars as pl


def compute_prreview_metrics(reviews_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute PR review metrics based on the submission date.
    """
    df = (
        reviews_df.sort("submitted_at")
        .group_by_dynamic(
            index_column="submitted_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",  # type: ignore
            include_boundaries=True,
        )
        .agg(
            [
                pl.count().alias("total_reviews"),
                pl.col("user").struct.field("login").n_unique().alias("unique_reviewers"),
            ]
        )
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
