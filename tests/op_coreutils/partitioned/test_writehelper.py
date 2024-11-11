import polars as pl
import pyarrow as pa
from datetime import date
from unittest.mock import patch


from op_coreutils.duckdb_local import run_query
from op_coreutils.partitioned.location import DataLocation
from op_coreutils.partitioned.writehelper import ParqueWriteManager
from op_coreutils.partitioned.output import ExpectedOutput, OutputData


def test_parquet_writer():
    run_query(
        "DELETE FROM etl_monitor.intermediate_model_markers WHERE chain IN ('DUMMYOP', 'DUMMYBASE')"
    )

    df = pl.DataFrame(
        {
            "dt": [
                date.fromisoformat("2024-01-01"),
                date.fromisoformat("2024-01-01"),
                date.fromisoformat("2024-01-01"),
                date.fromisoformat("2024-01-01"),
                date.fromisoformat("2024-01-02"),
                date.fromisoformat("2024-01-02"),
                date.fromisoformat("2024-01-03"),
            ],
            "chain": [
                "DUMMYOP",
                "DUMMYOP",
                "DUMMYBASE",
                "DUMMYBASE",
                "DUMMYBASE",
                "DUMMYBASE",
                "DUMMYOP",
            ],
            "c": ["some", "words", "here", "and", "few", "more", "blah"],
        }
    )

    manager = ParqueWriteManager(
        location=DataLocation.LOCAL,
        expected_output=ExpectedOutput(
            dataset_name="daily_address_summary/daily_address_summary_v1",
            root_path="intermediate/daily_address_summary/daily_address_summary_v1",
            file_name="out.parquet",
            marker_path="daily_address_summary/daily_address_summary_v1",
            process_name="default",
            additional_columns={"mode_name": "daily_address_summary"},
            additional_columns_schema=[
                pa.field("chain", pa.string()),
                pa.field("dt", pa.date32()),
                pa.field("model_name", pa.string()),
            ],
        ),
        markers_table="intermediate_model_markers",
        force=False,
    )

    with patch("op_coreutils.partitioned.writehelper.local_upload_parquet") as mock:
        manager.write(
            OutputData(
                dataframe=df,
                dataset_name="daily_address_summary/daily_address_summary_v1",
                default_partition=None,
            )
        )

    calls = []
    for mock_call in mock.call_args_list:
        calls.append(
            dict(
                path=mock_call.kwargs["path"],
                num_rows=len(mock_call.kwargs["df"]),
            )
        )

    calls.sort(key=lambda x: x["path"])

    assert calls == [
        {
            "path": "ozone/warehouse/intermediate/daily_address_summary/daily_address_summary_v1/chain=DUMMYBASE/dt=2024-01-01/out.parquet",
            "num_rows": 2,
        },
        {
            "path": "ozone/warehouse/intermediate/daily_address_summary/daily_address_summary_v1/chain=DUMMYBASE/dt=2024-01-02/out.parquet",
            "num_rows": 2,
        },
        {
            "path": "ozone/warehouse/intermediate/daily_address_summary/daily_address_summary_v1/chain=DUMMYOP/dt=2024-01-01/out.parquet",
            "num_rows": 2,
        },
        {
            "path": "ozone/warehouse/intermediate/daily_address_summary/daily_address_summary_v1/chain=DUMMYOP/dt=2024-01-03/out.parquet",
            "num_rows": 1,
        },
    ]
