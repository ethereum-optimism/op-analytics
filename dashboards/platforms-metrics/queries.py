github_pr_metrics = """
SELECT * FROM
dailydata_gcs.read_date(
    rootpath = 'github/github_repo_metrics_v1',
    dt = latest_dt('github/github_repo_metrics_v1')
)
SETTINGS use_hive_partitioning = 1
"""

github_pr_user_metrics = """
SELECT * FROM "transforms_github"."fact_user_reviews_v1"
"""
