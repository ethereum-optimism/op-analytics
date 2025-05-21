import pytest
from unittest.mock import patch, MagicMock


@patch("op_analytics.datasources.platform_metrics.execute.dt_summary")
@patch("op_analytics.datasources.platform_metrics.execute.PlatformMetrics")
@patch("op_analytics.datasources.platform_metrics.execute.write_pg_to_bq")
@patch("op_analytics.datasources.platform_metrics.execute.write_prom_to_bq")
@patch("op_analytics.datasources.platform_metrics.execute.PostgresDailyPull")
@patch("op_analytics.datasources.platform_metrics.execute.PrometheusDailyPull")
def test_execute_pull(
    mock_prom_pull,
    mock_pg_pull,
    mock_write_prom_to_bq,
    mock_write_pg_to_bq,
    mock_platform_metrics,
    mock_dt_summary,
):
    # Setup mocks for PostgresDailyPull
    mock_pg_instance = MagicMock()
    mock_pg_instance.jobs_df = "jobs_df"
    mock_pg_instance.jobs_df_truncated = "jobs_df_truncated"
    mock_pg_pull.fetch.return_value = mock_pg_instance

    # Setup mocks for PrometheusDailyPull
    mock_prom_instance = MagicMock()
    mock_prom_instance.metrics_df = "metrics_df"
    mock_prom_instance.metrics_df_truncated = "metrics_df_truncated"
    mock_prom_pull.fetch.return_value = mock_prom_instance

    # Setup mocks for BigQuery writers
    mock_write_pg_to_bq.return_value = "pg_bq_result"
    mock_write_prom_to_bq.return_value = "prom_bq_result"

    # Setup dt_summary
    mock_dt_summary.side_effect = lambda df: f"summary_of_{df}"

    # Call the function under test
    from op_analytics.datasources.platform_metrics.execute import execute_pull

    result = execute_pull()

    # Assert fetches
    mock_pg_pull.fetch.assert_called_once()
    mock_prom_pull.fetch.assert_called_once()

    # Assert BigQuery writes
    mock_write_pg_to_bq.assert_called_once_with(mock_pg_instance)
    mock_write_prom_to_bq.assert_called_once_with(mock_prom_instance)

    # Assert PlatformMetrics writes
    mock_platform_metrics.JOBS.write.assert_called_once_with(
        dataframe="jobs_df",
        sort_by=["dt", "id"],
    )
    mock_platform_metrics.PROMETHEUS_METRICS.write.assert_called_once_with(
        dataframe="metrics_df",
        sort_by=["dt", "metric"],
    )

    # Assert dt_summary calls
    mock_dt_summary.assert_any_call("jobs_df_truncated")
    mock_dt_summary.assert_any_call("metrics_df_truncated")

    # Assert result structure
    assert result == {
        "bigquery": {"pg": "pg_bq_result", "prom": "prom_bq_result"},
        "gcs": {
            "jobs_df": "summary_of_jobs_df_truncated",
            "metrics_df": "summary_of_metrics_df_truncated",
        },
    }
