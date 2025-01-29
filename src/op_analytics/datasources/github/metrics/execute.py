from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_date, date_tostr
from op_analytics.datasources.github.dataaccess import Github
from op_analytics.datasources.github.metrics.compute import compute_pr_metrics
import polars as pl

log = structlog.get_logger()


def execute_pull_pr_metrics(min_date: str, max_date: str):
    """
    Execute PR metrics computation pipeline:
    1. Load raw data from GCS
    2. Compute metrics
    3. Write metrics back to GCS
    """
    # 1. Load raw data directly as Polars DataFrames
    prs_df = Github.PRS.read_polars(min_date=min_date, max_date=max_date)
    comments_df = Github.PR_COMMENTS.read_polars(min_date=min_date, max_date=max_date)
    reviews_df = Github.PR_REVIEWS.read_polars(min_date=min_date, max_date=max_date)

    log.info(
        "loaded raw data from GCS",
        min_date=min_date,
        max_date=max_date,
        prs_count=len(prs_df),
        comments_count=len(comments_df),
        reviews_count=len(reviews_df),
    )

    metrics_df = compute_pr_metrics(
        prs_df=prs_df,
        comments_df=comments_df,
        reviews_df=reviews_df,
    )
    process_dt = date_tostr(now_date())

    Github.PR_METRICS.write(
        dataframe=metrics_df.with_columns(dt=pl.lit(process_dt)),
        sort_by=["repo"],
    )

    return {
        "process_dt": process_dt,
        "prs_processed": len(prs_df),
        "comments_processed": len(comments_df),
        "reviews_processed": len(reviews_df),
        "metrics_computed": len(metrics_df),
    }
