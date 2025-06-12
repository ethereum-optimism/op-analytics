from unittest.mock import patch, MagicMock
from datetime import date
from op_analytics.datasources.chainsmeta.systemconfig.execute import execute_pull


def test_execute_pull(monkeypatch):
    dummy_list = [MagicMock()]
    dummy_result = MagicMock()
    dummy_result.results = {"id": {"foo": "bar"}}
    dummy_result.failures = []
    with patch(
        "op_analytics.datasources.chainsmeta.systemconfig.execute.find_system_configs",
        return_value=dummy_list,
    ):
        with patch(
            "op_analytics.datasources.chainsmeta.systemconfig.execute.run_concurrently_store_failures",
            return_value=dummy_result,
        ):
            with patch(
                "op_analytics.datasources.chainsmeta.systemconfig.execute.SystemConfigList"
            ) as MockSCL:
                MockSCL.store_system_config_data.return_value = "stored"
                result = execute_pull(process_dt=date(2024, 1, 1))
                assert result == "stored"
