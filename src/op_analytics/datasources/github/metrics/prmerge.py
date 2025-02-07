import polars as pl


def compute_prmerge_metrics(prs_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute PR merge metrics based on the merge date.
    """
    df = (
        prs_df.filter(pl.col("merged_at").is_not_null())
        .sort("merged_at")
        .group_by_dynamic(
            index_column="merged_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",  # type: ignore
            include_boundaries=True,
        )
        .agg([pl.col("pr_number").n_unique().alias("merged_prs")])
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
