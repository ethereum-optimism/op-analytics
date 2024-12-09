import json
from unittest.mock import patch

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.blockbatch.construct import construct_tasks
from op_analytics.datapipeline.etl.blockbatch.task import BlockBatchModelsTask
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.coreutils.partitioned.partition import Partition, PartitionColumn


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
        tasks = construct_tasks(
            chains=["mode"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )

    assert tasks == [
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016416000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016416000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16416000,
                    "max_block": 16424000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16416000,
                    "max_block": 16424000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016416000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016416000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016424000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016424000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16424000,
                    "max_block": 16432000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16424000,
                    "max_block": 16432000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016424000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016424000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016432000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016432000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16432000,
                    "max_block": 16440000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16432000,
                    "max_block": 16440000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016432000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016432000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016440000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016440000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16440000,
                    "max_block": 16448000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16440000,
                    "max_block": 16448000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016440000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016440000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016448000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016448000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16448000,
                    "max_block": 16456000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16448000,
                    "max_block": 16456000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016448000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016448000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016456000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016456000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16456000,
                    "max_block": 16464000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16456000,
                    "max_block": 16464000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016456000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016456000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model="contract_creation",
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="mode"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016464000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016464000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 8000,
                    "min_block": 16464000,
                    "max_block": 16472000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 8000,
                    "min_block": 16464000,
                    "max_block": 16472000,
                },
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("num_blocks", pa.int32()),
                    pa.field("min_block", pa.int64()),
                    pa.field("max_block", pa.int64()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="blockbatch_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="blockbatch/contract_creation/create_traces_v1",
                        file_name="000016464000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016464000",
                    )
                ],
                force=False,
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
    ]
