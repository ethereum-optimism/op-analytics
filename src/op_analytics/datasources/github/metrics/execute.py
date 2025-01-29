from op_analytics.coreutils.logger import structlog
from op_analytics.datasources.github.dataaccess import Github
from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.datasources.github.metrics.compute import compute_pr_metrics

log = structlog.get_logger()


def execute_pull_pr_metrics(min_date: str, max_date: str):
    """
    Execute PR metrics computation pipeline:
    1. Load raw data from GCS
    2. Compute metrics
    3. Write metrics back to GCS
    """
    # 1. Load raw data from GCS with date filters
    prs_view = Github.PRS.read(min_date=min_date, max_date=max_date)
    comments_view = Github.PR_COMMENTS.read(min_date=min_date, max_date=max_date)
    reviews_view = Github.PR_REVIEWS.read(min_date=min_date, max_date=max_date)

    # Convert views to DataFrames using DuckDB
    duckdb_ctx = init_client()
    prs_df = duckdb_ctx.client.sql(f"SELECT * FROM {prs_view}").pl()
    comments_df = duckdb_ctx.client.sql(f"SELECT * FROM {comments_view}").pl()
    reviews_df = duckdb_ctx.client.sql(f"SELECT * FROM {reviews_view}").pl()

    log.info(
        "loaded raw data from GCS",
        prs_count=len(prs_df),
        comments_count=len(comments_df),
        reviews_count=len(reviews_df),
    )

    # 2. Compute metrics using the compute module
    metrics_df = compute_pr_metrics(
        prs_df=prs_df,
        comments_df=comments_df,
        reviews_df=reviews_df,
    )

    # 3. Write computed metrics to GCS
    Github.PR_METRICS.write(dataframe=metrics_df, sort_by=["dt", "repo"])

    return {
        "prs_processed": len(prs_df),
        "comments_processed": len(comments_df),
        "reviews_processed": len(reviews_df),
        "metrics_computed": len(metrics_df),
    }


def execute_compute_pr_metrics() -> dict:
    """Compute PR metrics for the last 30 days.

    Computes daily metrics about PR activity and performance across repos

    Returns:
        dict: Summary of processed items including counts of PRs, comments,
        reviews and metrics computed.
    """
    from op_analytics.coreutils.time import now_date, date_tostr, datestr_subtract

    # Compute date range
    end_date = date_tostr(now_date())
    start_date = datestr_subtract(end_date, 30)

    return execute_pull_pr_metrics(
        min_date=start_date,
        max_date=end_date,
    )
