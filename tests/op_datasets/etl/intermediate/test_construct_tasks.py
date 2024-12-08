import json
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pytest

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import Partition, PartitionColumn
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.intermediate.construct import construct_tasks
from op_analytics.datapipeline.etl.intermediate.task import IntermediateModelsTask


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


def test_construct_mixed_chains():
    with pytest.raises(ValueError) as ex:
        construct_tasks(
            chains=["mode", "unichain_sepolia"],
            models=["contract_creation"],
            range_spec="@20241201:+1",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
        )
    assert ex.value.args == (
        "could not determine MAINNET/TESTNET for chains: ['mode', 'unichain_sepolia']",
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
            model="contract_creation",
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
                        process_name="default",
                    )
                ],
                force=False,
            ),
            output_root_path_prefix="intermediate",
        )
    ]


def test_construct_testnet():
    with patch("op_analytics.coreutils.partitioned.dataaccess.run_query_oplabs") as m1:
        m1.return_value = make_dataframe("testnet_markers.json")

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
            model="contract_creation",
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
                        process_name="default",
                    )
                ],
                force=False,
            ),
            output_root_path_prefix="intermediate_testnets",
        )
    ]
