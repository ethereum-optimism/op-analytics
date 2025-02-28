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
from op_analytics.datapipeline.etl.ingestion.reader.ranges import ChainMaxBlock, BlockRange
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath


def make_dataframe(path: str):
    with open(InputTestData.at(__file__).path(f"testdata/{path}")) as fobj:
        return pl.DataFrame(
            json.load(fobj),
            schema={
                "dt": pl.UInt16(),
                "chain": pl.String(),
                "marker_path": pl.String(),
                "num_parts": pl.UInt32(),
                "num_blocks": pl.Int32(),
                "min_block": pl.Int64(),
                "max_block": pl.Int64(),
                "data_path": pl.String(),
                "root_path": pl.String(),
            },
        )


def mock_block_range(chain: str, min_ts: int, max_ts: int):
    if chain == "mode":
        return BlockRange(min=16421809, max=16465008)

    if chain == "unichain_sepolia":
        return BlockRange(min=6158772, max=6245171)

    if chain == "kroma":
        return BlockRange(min=18269407, max=18312606)

    raise NotImplementedError()


def mock_max_block(chain: str):
    if chain == "mode":
        return ChainMaxBlock(chain="mode", ts=1736391057, number=18111737)

    if chain == "unichain_sepolia":
        return ChainMaxBlock(chain="unichain_sepolia", ts=1736391053, number=9538625)

    if chain == "kroma":
        return ChainMaxBlock(chain="kroma", ts=1736391287, number=21255450)


def test_construct():
    with (
        patch(
            "op_analytics.datapipeline.etl.ingestion.reader.ranges.block_range_for_dates",
            new=mock_block_range,
        ),
        patch(
            "op_analytics.datapipeline.etl.ingestion.reader.ranges.chain_max_block",
            new=mock_max_block,
        ),
        patch("op_analytics.coreutils.partitioned.markers_clickhouse.run_query_oplabs") as m1,
    ):
        m1.return_value = make_dataframe("mainnet_markers.json")

        tasks = construct_date_load_tasks(
            chains=["mode"],
            range_spec="@20241201:+1",
            root_paths_to_read=[
                RootPath.of("ingestion/blocks_v1"),
                RootPath.of("ingestion/logs_v1"),
                RootPath.of("ingestion/traces_v1"),
                RootPath.of("ingestion/transactions_v1"),
            ],
            write_to=DataLocation.GCS,
            bq_dataset_name="dummy_dataset",
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
