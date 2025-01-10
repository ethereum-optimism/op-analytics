import json
from unittest.mock import patch

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import Partition, PartitionColumn
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.intermediate.construct import construct_tasks
from op_analytics.datapipeline.etl.intermediate.task import IntermediateModelsTask
from op_analytics.datapipeline.models.compute.execute import PythonModel
from op_analytics.datapipeline.etl.ingestion.reader.ranges import ChainMaxBlock, BlockRange


SCHEMA = {
    "dt": pl.UInt16(),
    "chain": pl.String(),
    "marker_path": pl.String(),
    "num_parts": pl.UInt32(),
    "num_blocks": pl.Int32(),
    "min_block": pl.Int64(),
    "max_block": pl.Int64(),
    "data_path": pl.String(),
    "root_path": pl.String(),
}


def make_dataframe(path: str):
    with open(InputTestData.at(__file__).path(f"../testdata/{path}")) as fobj:
        return pl.DataFrame(json.load(fobj), schema=SCHEMA)


def make_empty_dataframe():
    return pl.DataFrame([], schema=SCHEMA)


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


def test_construct_mixed_chains():
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
        m1.side_effect = [
            pl.concat(
                [
                    make_dataframe("ingestion_mode_markers.json"),
                    make_dataframe("ingestion_unichain_sepolia_markers.json"),
                ]
            ),
            make_dataframe("intermediate_mode_markers.json"),
        ]

        tasks = construct_tasks(
            chains=["mode", "unichain_sepolia"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )

    assert tasks == [
        IntermediateModelsTask(
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
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                },
                inputs_ready=True,
            ),
            model=PythonModel.get("contract_creation"),
            output_duckdb_relations={},
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={"model_name": "contract_creation"},
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="intermediate_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="intermediate/contract_creation/create_traces_v1",
                        file_name="out.parquet",
                        marker_path="2024-12-01/mode/contract_creation/create_traces_v1",
                    )
                ],
                complete_markers=["2024-12-01/mode/contract_creation/create_traces_v1"],
            ),
            output_root_path_prefix="intermediate",
        ),
        IntermediateModelsTask(
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="unichain_sepolia"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006155000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006160000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006165000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006170000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006175000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006180000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006185000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006190000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006195000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006200000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006205000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006210000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006215000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006220000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006225000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006230000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006235000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006240000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006245000.parquet",
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006155000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006160000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006165000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006170000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006175000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006180000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006185000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006190000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006195000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006200000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006205000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006210000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006215000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006220000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006225000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006230000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006235000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006240000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006245000.parquet",
                    ],
                },
                inputs_ready=True,
            ),
            model=PythonModel.get("contract_creation"),
            output_duckdb_relations={},
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={"model_name": "contract_creation"},
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="intermediate_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="intermediate_testnets/contract_creation/create_traces_v1",
                        file_name="out.parquet",
                        marker_path="2024-12-01/unichain_sepolia/contract_creation/create_traces_v1",
                    )
                ],
                complete_markers=[],
            ),
            output_root_path_prefix="intermediate_testnets",
        ),
    ]


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
        m1.side_effect = [
            make_dataframe("ingestion_mode_markers.json"),
            make_empty_dataframe(),
        ]

        tasks = construct_tasks(
            chains=["mode"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )

    assert tasks == [
        IntermediateModelsTask(
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
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016416000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016424000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016432000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016440000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016448000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016456000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=mode/dt=2024-12-01/000016464000.parquet",
                    ],
                },
                inputs_ready=True,
            ),
            model=PythonModel.get("contract_creation"),
            output_duckdb_relations={},
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={"model_name": "contract_creation"},
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="intermediate_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="intermediate/contract_creation/create_traces_v1",
                        file_name="out.parquet",
                        marker_path="2024-12-01/mode/contract_creation/create_traces_v1",
                    )
                ],
                complete_markers=[],
            ),
            output_root_path_prefix="intermediate",
        )
    ]


def test_construct_testnet():
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
        m1.side_effect = [
            make_dataframe("ingestion_unichain_sepolia_markers.json"),
            make_empty_dataframe(),
        ]

        tasks = construct_tasks(
            chains=["unichain_sepolia"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )

    assert tasks == [
        IntermediateModelsTask(
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="unichain_sepolia"),
                        PartitionColumn(name="dt", value="2024-12-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006155000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006160000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006165000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006170000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006175000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006180000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006185000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006190000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006195000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006200000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006205000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006210000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006215000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006220000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006225000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006230000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006235000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006240000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006245000.parquet",
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006155000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006160000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006165000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006170000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006175000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006180000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006185000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006190000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006195000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006200000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006205000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006210000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006215000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006220000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006225000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006230000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006235000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006240000.parquet",
                        "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006245000.parquet",
                    ],
                },
                inputs_ready=True,
            ),
            model=PythonModel.get("contract_creation"),
            output_duckdb_relations={},
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={"model_name": "contract_creation"},
                extra_marker_columns_schema=[
                    pa.field("chain", pa.string()),
                    pa.field("dt", pa.date32()),
                    pa.field("model_name", pa.string()),
                ],
                markers_table="intermediate_model_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="intermediate_testnets/contract_creation/create_traces_v1",
                        file_name="out.parquet",
                        marker_path="2024-12-01/unichain_sepolia/contract_creation/create_traces_v1",
                    )
                ],
                complete_markers=[],
            ),
            output_root_path_prefix="intermediate_testnets",
        )
    ]
