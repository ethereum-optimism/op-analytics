import polars as pl


def compute_prapproval_metrics(prs_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute PR approval metrics based on the approved_at timestamp.
    """
    df = (
        prs_df.filter(pl.col("approved_at").is_not_null())
        .sort("approved_at")
        .group_by_dynamic(
            index_column="approved_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",
            include_boundaries=True,
        )
        .agg(pl.col("pr_number").n_unique().alias("approved_prs"))
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
