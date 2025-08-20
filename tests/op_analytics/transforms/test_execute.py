import datetime
from unittest.mock import patch, MagicMock

import polars as pl
from clickhouse_connect.driver.summary import QuerySummary

from op_analytics.coreutils.logger import structlog
from op_analytics.transforms.create import TableStructure, TableColumn
from op_analytics.transforms.task import TransformTask

log = structlog.get_logger()


@patch("op_analytics.transforms.task.new_stateful_client")
@patch("op_analytics.transforms.task.insert_oplabs")
def test_execute_task(mock_insert: MagicMock, mock_new: MagicMock):
    mock_new().command.return_value = QuerySummary({"written_rows": 100})

    task = TransformTask(
        group_name="interop",
        dt=datetime.date(2023, 1, 1),
        tables={
            "fact_erc20_create_traces_v2": TableStructure(
                name="fact_erc20_create_traces_v2",
                columns=[TableColumn(name="dummy", data_type="String")],
            ),
        },
        steps_to_run=[6],
        steps_to_skip=None,
        raise_if_empty=False,
    )

    task.execute()

    clickhouse_commands = mock_new().command.call_args_list
    calls = []
    for call in clickhouse_commands:
        assert isinstance(call.kwargs["cmd"], str)
        calls.append(
            {
                "parameters": call.kwargs["parameters"],
                "settings": call.kwargs["settings"],
            }
        )

    assert calls == [
        {
            "parameters": {"dtparam": datetime.date(2023, 1, 1)},
            "settings": {"use_hive_partitioning": 1},
        }
    ]

    inserts = []

    for insert in mock_insert.call_args_list:
        rows = pl.from_arrow(insert.kwargs["df_arrow"]).to_dicts()  # type: ignore

        # Delete non-determnistics values.
        for row in rows:
            del row["writer_name"]

        inserts.append(
            {
                "database": insert.kwargs["database"],
                "table": insert.kwargs["table"],
                "rows": rows,
            }
        )

    assert inserts == [
        {
            "database": "etl_monitor",
            "table": "transform_dt_markers",
            "rows": [
                {
                    "transform": "interop",
                    "dt": datetime.date(2023, 1, 1),
                    "metadata": '[{"name": "06_fact_erc20_create_traces_v2.sql", "result": {"written_rows": 100}}]',
                    "process_name": "default",
                }
            ],
        }
    ]
