from unittest.mock import patch, MagicMock


def test_execute_pull():
    # Patch all dependencies used in execute_pull
    with (
        patch(
            "op_analytics.datasources.platform_metrics.execute.PostgresDailyPull"
        ) as mock_pg_pull,
        patch(
            "op_analytics.datasources.platform_metrics.execute.PrometheusDailyPull"
        ) as mock_prom_pull,
        patch(
            "op_analytics.datasources.platform_metrics.execute.PlatformMetrics"
        ) as mock_platform_metrics,
        patch("op_analytics.datasources.platform_metrics.execute.dt_summary") as mock_dt_summary,
    ):
        # Mock PostgresDailyPull.fetch()
        mock_pg_instance = MagicMock()
        mock_pg_instance.jobs_df = "jobs_df"
        mock_pg_instance.pipelines_df = "pipelines_df"
        mock_pg_pull.fetch.return_value = mock_pg_instance

        # Mock PrometheusDailyPull.fetch()
        mock_prom_instance = MagicMock()
        mock_prom_instance.metrics_df = "metrics_df"
        mock_prom_pull.fetch.return_value = mock_prom_instance

        # Mock dt_summary
        mock_dt_summary.side_effect = lambda df: f"summary_of_{df}"

        # Import and call the function under test
        from op_analytics.datasources.platform_metrics.execute import execute_pull

        result = execute_pull()

        # Assert fetches
        mock_pg_pull.fetch.assert_called_once()
        mock_prom_pull.fetch.assert_called_once()

        # Assert writes
        mock_platform_metrics.JOBS.write.assert_called_once_with(
            dataframe="jobs_df",
            sort_by=["dt", "id"],
        )
        mock_platform_metrics.PIPELINES.write.assert_called_once_with(
            dataframe="pipelines_df",
            sort_by=["dt", "id"],
        )
        mock_platform_metrics.PROMETHEUS_METRICS.write.assert_called_once_with(
            dataframe="metrics_df",
            sort_by=["dt", "metric"],
        )

        # Assert dt_summary calls
        mock_dt_summary.assert_any_call("jobs_df")
        mock_dt_summary.assert_any_call("pipelines_df")
        mock_dt_summary.assert_any_call("metrics_df")

        # Assert result structure
        assert result == {
            "gcs": {
                "jobs_df": "summary_of_jobs_df",
                "pipelines_df": "summary_of_pipelines_df",
                "metrics_df": "summary_of_metrics_df",
            }
        }
