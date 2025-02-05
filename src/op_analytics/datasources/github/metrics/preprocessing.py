import polars as pl
from op_analytics.coreutils.time import parse_isoformat


def prepare_prs(prs_df: pl.DataFrame) -> pl.DataFrame:
    prs_df = prs_df.rename({"number": "pr_number"})
    prs_df = deduplicate_prs(prs_df)
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
