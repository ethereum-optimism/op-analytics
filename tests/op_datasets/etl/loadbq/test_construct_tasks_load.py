import datetime
import json
from unittest.mock import patch

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.loadbq.construct import construct_date_load_tasks
from op_analytics.datapipeline.etl.loadbq.loader import BQLoader, BQOutputData
from op_analytics.datapipeline.etl.loadbq.task import DateLoadTask


def make_dataframe(path: str):
    with open(InputTestData.at(__file__).path(f"testdata/{path}")) as fobj:
        return pl.DataFrame(
            json.load(fobj),
            schema={
                "dt": pl.UInt16(),
                "chain": pl.String(),
                "num_blocks": pl.Int32(),
                "min_block": pl.Int64(),
                "max_block": pl.Int64(),
                "data_path": pl.String(),
                "root_path": pl.String(),
            },
        )


def test_construct():
    with patch("op_analytics.coreutils.partitioned.dataaccess.run_query_oplabs") as m1:
        m1.return_value = make_dataframe("mainnet_markers.json")

        tasks = construct_date_load_tasks(
            chains=["mode"],
            range_spec="@20241201:+1",
            write_to=DataLocation.GCS,
            bq_dataset_name="dummy_dataset",
            force_complete=False,
        )

    assert tasks == [
        DateLoadTask(
            dateval=datetime.date(2024, 12, 1),
            chains_ready={"mode"},
            chains_not_ready=set(),
            write_manager=BQLoader(
                location=DataLocation.GCS,
                partition_cols=["dt"],
                extra_marker_columns={},
                extra_marker_columns_schema=[
                    pa.field("dt", pa.date32()),
                ],
                markers_table="superchain_raw_bigquery_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="dummy_dataset/blocks",
                        file_name="",
                        marker_path="dummy_dataset/blocks/2024-12-01",
                    ),
                    ExpectedOutput(
                        root_path="dummy_dataset/logs",
                        file_name="",
                        marker_path="dummy_dataset/logs/2024-12-01",
                    ),
                    ExpectedOutput(
                        root_path="dummy_dataset/traces",
                        file_name="",
                        marker_path="dummy_dataset/traces/2024-12-01",
                    ),
                    ExpectedOutput(
                        root_path="dummy_dataset/transactions",
                        file_name="",
                        marker_path="dummy_dataset/transactions/2024-12-01",
                    ),
                ],
                force=False,
                process_name="default",
            ),
            outputs=[
                BQOutputData(
                    root_path="dummy_dataset/blocks",
                    source_uris=[
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/blocks_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                    source_uris_root_path="gs://oplabs-tools-data-sink/ingestion/blocks_v1/",
                    dateval=datetime.date(2024, 12, 1),
                    bq_dataset_name="dummy_dataset",
                    bq_table_name="blocks",
                ),
                BQOutputData(
                    root_path="dummy_dataset/logs",
                    source_uris=[
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/logs_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                    source_uris_root_path="gs://oplabs-tools-data-sink/ingestion/logs_v1/",
                    dateval=datetime.date(2024, 12, 1),
                    bq_dataset_name="dummy_dataset",
                    bq_table_name="logs",
                ),
                BQOutputData(
                    root_path="dummy_dataset/traces",
                    source_uris=[
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                    source_uris_root_path="gs://oplabs-tools-data-sink/ingestion/traces_v1/",
                    dateval=datetime.date(2024, 12, 1),
                    bq_dataset_name="dummy_dataset",
                    bq_table_name="traces",
                ),
                BQOutputData(
                    root_path="dummy_dataset/transactions",
                    source_uris=[
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                    source_uris_root_path="gs://oplabs-tools-data-sink/ingestion/transactions_v1/",
                    dateval=datetime.date(2024, 12, 1),
                    bq_dataset_name="dummy_dataset",
                    bq_table_name="transactions",
                ),
            ],
        )
    ]
