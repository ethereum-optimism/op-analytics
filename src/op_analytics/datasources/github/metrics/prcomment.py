import polars as pl


def compute_prcomment_metrics(comments_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute PR comment metrics based on the comment creation date.
    """
    df = (
        comments_df.sort("created_at")
        .group_by_dynamic(
            index_column="created_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",
            include_boundaries=True,
        )
        .agg(
            [
                pl.count().alias("total_comments"),
                pl.col("user").struct.field("login").n_unique().alias("unique_commenters"),
            ]
        )
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
