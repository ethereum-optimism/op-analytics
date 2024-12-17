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
from op_analytics.datapipeline.models.compute.execute import PythonModel


def make_dataframe(path: str):
    with open(InputTestData.at(__file__).path(f"../testdata/{path}")) as fobj:
        # The markers testdata used for intermediate models includes padded dates,
        # so it does not match the query that we are mocking to get markers. We
        # filter the data to a single data so that the mock is correct.
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
        ).filter(pl.col("dt") == 20058)


def test_construct():
    with patch("op_analytics.coreutils.partitioned.markers_clickhouse.run_query_oplabs") as m1:
        m1.side_effect = [
            # Mock data for ingestion markers. This is used to create the data readers.
            pl.concat(
                [
                    make_dataframe("ingestion_mode_markers.json"),
                    make_dataframe("ingestion_unichain_sepolia_markers.json"),
                ]
            ),
            # Mock data for blockbach markers. This is used to set complete_markers on
            # the write managers.
            make_dataframe("blockbatch_mode_markers.json"),
        ]

        tasks = construct_tasks(
            chains=["mode", "unichain_sepolia"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )

    mainnet_tasks = [_ for _ in tasks if _.output_root_path_prefix == "blockbatch"]
    testnet_tasks = [_ for _ in tasks if _.output_root_path_prefix == "blockbatch_testnets"]

    assert len(mainnet_tasks) == 7
    assert len(testnet_tasks) == 19

    assert mainnet_tasks == [
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016416000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016424000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016432000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016440000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016448000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016456000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
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
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/mode/2024-12-01/000016464000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
    ]

    assert testnet_tasks[0] == BlockBatchModelsTask(
        model=PythonModel.get("contract_creation"),
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
                    "gs://oplabs-tools-data-sink/ingestion_testnets/traces_v1/chain=unichain_sepolia/dt=2024-12-01/000006155000.parquet"
                ],
                "ingestion/transactions_v1": [
                    "gs://oplabs-tools-data-sink/ingestion_testnets/transactions_v1/chain=unichain_sepolia/dt=2024-12-01/000006155000.parquet"
                ],
            },
            inputs_ready=True,
            extra_marker_data={"num_blocks": 5000, "min_block": 6155000, "max_block": 6160000},
        ),
        write_manager=PartitionedWriteManager(
            location=DataLocation.GCS,
            partition_cols=["chain", "dt"],
            extra_marker_columns={
                "model_name": "contract_creation",
                "num_blocks": 5000,
                "min_block": 6155000,
                "max_block": 6160000,
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
                    root_path="blockbatch_testnets/contract_creation/create_traces_v1",
                    file_name="000006155000.parquet",
                    marker_path="blockbatch_testnets/contract_creation/create_traces_v1/unichain_sepolia/2024-12-01/000006155000",
                )
            ],
            complete_markers=[],
            process_name="default",
        ),
        output_duckdb_relations={},
        output_root_path_prefix="blockbatch_testnets",
    )


def test_construct_kroma():
    # with patch("op_analytics.coreutils.partitioned.markers_clickhouse.run_query_oplabs") as m1:
    #     m1.side_effect = [
    #         # Mock data for ingestion markers. This is used to create the data readers.
    #         pl.concat(
    #             [
    #                 make_dataframe("ingestion_mode_markers.json"),
    #                 make_dataframe("ingestion_unichain_sepolia_markers.json"),
    #             ]
    #         ),
    #         # Mock data for blockbach markers. This is used to set complete_markers on
    #         # the write managers.
    #         make_dataframe("blockbatch_mode_markers.json"),
    #     ]

    tasks = construct_tasks(
        chains=["kroma"],
        models=["contract_creation"],
        range_spec="@20241101:+1",
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
    )

    assert tasks == [
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="kroma"),
                        PartitionColumn(name="dt", value="2024-11-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={},
                inputs_ready=False,
                extra_marker_data={
                    "num_blocks": 20000,
                    "min_block": 18260000,
                    "max_block": 18280000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 20000,
                    "min_block": 18260000,
                    "max_block": 18280000,
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
                        file_name="000018260000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/kroma/2024-11-01/000018260000",
                    )
                ],
                complete_markers=[],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="kroma"),
                        PartitionColumn(name="dt", value="2024-11-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=kroma/dt=2024-11-01/000018280000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=kroma/dt=2024-11-01/000018280000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 20000,
                    "min_block": 18280000,
                    "max_block": 18300000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 20000,
                    "min_block": 18280000,
                    "max_block": 18300000,
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
                        file_name="000018280000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/kroma/2024-11-01/000018280000",
                    )
                ],
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/kroma/2024-11-01/000018280000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
        BlockBatchModelsTask(
            model=PythonModel.get("contract_creation"),
            data_reader=DataReader(
                partitions=Partition(
                    cols=[
                        PartitionColumn(name="chain", value="kroma"),
                        PartitionColumn(name="dt", value="2024-11-01"),
                    ]
                ),
                read_from=DataLocation.GCS,
                dataset_paths={
                    "ingestion/traces_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=kroma/dt=2024-11-01/000018300000.parquet"
                    ],
                    "ingestion/transactions_v1": [
                        "gs://oplabs-tools-data-sink/ingestion/transactions_v1/chain=kroma/dt=2024-11-01/000018300000.parquet"
                    ],
                },
                inputs_ready=True,
                extra_marker_data={
                    "num_blocks": 20000,
                    "min_block": 18300000,
                    "max_block": 18320000,
                },
            ),
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
                    "model_name": "contract_creation",
                    "num_blocks": 20000,
                    "min_block": 18300000,
                    "max_block": 18320000,
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
                        file_name="000018300000.parquet",
                        marker_path="blockbatch/contract_creation/create_traces_v1/kroma/2024-11-01/000018300000",
                    )
                ],
                complete_markers=[
                    "blockbatch/contract_creation/create_traces_v1/kroma/2024-11-01/000018300000"
                ],
                process_name="default",
            ),
            output_duckdb_relations={},
            output_root_path_prefix="blockbatch",
        ),
    ]
