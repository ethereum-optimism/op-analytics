import polars as pl


def compute_prupdate_metrics(prs_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute PR update metrics based on the updated_at timestamp.
    """
    df = (
        prs_df.filter(pl.col("updated_at").is_not_null())
        .sort("updated_at")
        .group_by_dynamic(
            index_column="updated_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",  # type: ignore
            include_boundaries=True,
        )
        .agg(pl.col("pr_number").n_unique().alias("active_prs"))
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
