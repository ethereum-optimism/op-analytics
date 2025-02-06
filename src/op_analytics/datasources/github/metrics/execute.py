from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_dt
from op_analytics.datasources.github.dataaccess import Github
from op_analytics.datasources.github.metrics.compute import compute_all_metrics
import polars as pl

log = structlog.get_logger()


def execute_pull_repo_metrics():
    """
    Execute the PR metrics computation pipeline:
      1. Load raw data from GCS.
      2. Compute rolling/detailed metrics.
      3. Write the computed metrics to the REPO_METRICS dataset.

    We read all of the historical data and recompute metrics from scratch. Results are saved under a "dt"
    partition that indicates the processing date but is not related to the data.
    Returns:
        dict: Summary of processing results.
    """
    prs_df = Github.PRS.read_polars()
    comments_df = Github.PR_COMMENTS.read_polars()
    reviews_df = Github.PR_REVIEWS.read_polars()

    log.info(
        "Loaded raw GitHub data",
        prs_count=len(prs_df),
        comments_count=len(comments_df),
        reviews_count=len(reviews_df),
    )

    process_dt = now_dt()

    metrics_df = compute_all_metrics(
        prs_df=prs_df,
        comments_df=comments_df,
        reviews_df=reviews_df,
    )

    Github.REPO_METRICS.write(
        dataframe=metrics_df.with_columns(dt=pl.lit(process_dt)),
        sort_by=["repo", "period_start"],
    )

    log.info(
        "Completed PR metrics computation and storage",
        metrics_computed=len(metrics_df),
    )

    return {
        "process_dt": process_dt,
        "prs_processed": len(prs_df),
        "comments_processed": len(comments_df),
        "reviews_processed": len(reviews_df),
        "metrics_computed": len(metrics_df),
    }
