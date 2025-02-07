import polars as pl


def compute_prclose_metrics(prs_df: pl.DataFrame, period_str: str) -> pl.DataFrame:
    """
    Compute PR close metrics based on the closed_at date.
    """
    df = (
        prs_df.filter(pl.col("closed_at").is_not_null())
        .sort("closed_at")
        .group_by_dynamic(
            index_column="closed_at",
            every="1d",
            period=period_str,
            closed="left",
            by="repo",  # type: ignore
            include_boundaries=True,
        )
        .agg(
            [
                pl.col("pr_number").n_unique().alias("closed_prs"),
                pl.col("pr_number")
                .filter(pl.col("merged_at").is_null())
                .n_unique()
                .alias("rejected_prs"),
            ]
        )
        .rename({"_lower_boundary": "period_start", "_upper_boundary": "period_end"})
    )
    return df
