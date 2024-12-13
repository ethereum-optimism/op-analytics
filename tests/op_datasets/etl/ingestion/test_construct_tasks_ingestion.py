import json
from unittest.mock import patch

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.writer import PartitionedWriteManager
from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.ingestion.construct import construct_tasks
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider
from op_analytics.datapipeline.etl.ingestion.task import IngestionTask
from op_analytics.datapipeline.etl.ingestion.batches import BlockBatch


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
    with patch("op_analytics.datapipeline.etl.ingestion.construct.block_range_for_dates") as m1:
        m1.return_value = BlockRange(min=16421809, max=16439980)

        tasks = construct_tasks(
            chains=["mode"],
            range_spec="@20241201:+1",
            read_from=RawOnchainDataProvider.GOLDSKY,
            write_to=DataLocation.GCS,
        )

    assert tasks == [
        IngestionTask(
            max_requested_timestamp=1733097600,
            block_batch=BlockBatch(chain="mode", min=16416000, max=16424000),
            read_from=RawOnchainDataProvider.GOLDSKY,
            input_datasets={},
            input_dataframes={},
            output_dataframes=[],
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
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
                ],
                markers_table="raw_onchain_ingestion_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="ingestion/blocks_v1",
                        file_name="000016416000.parquet",
                        marker_path="ingestion/blocks_v1/mode/000016416000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/transactions_v1",
                        file_name="000016416000.parquet",
                        marker_path="ingestion/transactions_v1/mode/000016416000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/logs_v1",
                        file_name="000016416000.parquet",
                        marker_path="ingestion/logs_v1/mode/000016416000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/traces_v1",
                        file_name="000016416000.parquet",
                        marker_path="ingestion/traces_v1/mode/000016416000",
                    ),
                ],
            ),
            progress_indicator="",
        ),
        IngestionTask(
            max_requested_timestamp=1733097600,
            block_batch=BlockBatch(chain="mode", min=16424000, max=16432000),
            read_from=RawOnchainDataProvider.GOLDSKY,
            input_datasets={},
            input_dataframes={},
            output_dataframes=[],
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
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
                ],
                markers_table="raw_onchain_ingestion_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="ingestion/blocks_v1",
                        file_name="000016424000.parquet",
                        marker_path="ingestion/blocks_v1/mode/000016424000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/transactions_v1",
                        file_name="000016424000.parquet",
                        marker_path="ingestion/transactions_v1/mode/000016424000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/logs_v1",
                        file_name="000016424000.parquet",
                        marker_path="ingestion/logs_v1/mode/000016424000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/traces_v1",
                        file_name="000016424000.parquet",
                        marker_path="ingestion/traces_v1/mode/000016424000",
                    ),
                ],
            ),
            progress_indicator="",
        ),
        IngestionTask(
            max_requested_timestamp=1733097600,
            block_batch=BlockBatch(chain="mode", min=16432000, max=16440000),
            read_from=RawOnchainDataProvider.GOLDSKY,
            input_datasets={},
            input_dataframes={},
            output_dataframes=[],
            write_manager=PartitionedWriteManager(
                location=DataLocation.GCS,
                partition_cols=["chain", "dt"],
                extra_marker_columns={
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
                ],
                markers_table="raw_onchain_ingestion_markers",
                expected_outputs=[
                    ExpectedOutput(
                        root_path="ingestion/blocks_v1",
                        file_name="000016432000.parquet",
                        marker_path="ingestion/blocks_v1/mode/000016432000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/transactions_v1",
                        file_name="000016432000.parquet",
                        marker_path="ingestion/transactions_v1/mode/000016432000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/logs_v1",
                        file_name="000016432000.parquet",
                        marker_path="ingestion/logs_v1/mode/000016432000",
                    ),
                    ExpectedOutput(
                        root_path="ingestion/traces_v1",
                        file_name="000016432000.parquet",
                        marker_path="ingestion/traces_v1/mode/000016432000",
                    ),
                ],
            ),
            progress_indicator="",
        ),
    ]
