import json
from unittest.mock import patch

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.writerpartitioned import PartitionedWriteManager
from op_analytics.coreutils.rangeutils.blockrange import BlockRange, ChainMaxBlock
from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.etl.ingestion.construct import construct_tasks, overlapping_markers
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider
from op_analytics.datapipeline.etl.ingestion.task import IngestionTask
from op_analytics.datapipeline.etl.ingestion.batches import BlockBatch


def make_dataframe(path: str):
    with open(InputTestData.at(__file__).path(f"../testdata/{path}")) as fobj:
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


def test_construct():
    with (
        patch("op_analytics.datapipeline.etl.ingestion.reader.ranges.block_range_for_dates") as m1,
        patch("op_analytics.datapipeline.etl.ingestion.reader.ranges.chain_max_block") as m2,
        patch("op_analytics.coreutils.partitioned.markers_clickhouse.run_query_oplabs") as m3,
    ):
        m1.return_value = BlockRange(min=16421809, max=16439980)
        m2.return_value = ChainMaxBlock(chain="mode", ts=1736362200, number=18097309)
        m3.return_value = make_dataframe("ingestion_mode_markers_padded_dates.json")

        tasks = construct_tasks(
            chains=["mode"],
            range_spec="@20241201:+1",
            read_from=RawOnchainDataProvider.GOLDSKY,
            write_to=DataLocation.GCS,
        )

    assert tasks == [
        IngestionTask(
            chain_max_block=ChainMaxBlock(chain="mode", ts=1736362200, number=18097309),
            requested_max_timestamp=1733097600,
            block_batch=BlockBatch(chain="mode", min=16416000, max=16424000),
            read_from=RawOnchainDataProvider.GOLDSKY,
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
                markers_table="blockbatch_markers",
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
                complete_markers=[
                    "ingestion/blocks_v1/mode/000016416000",
                    "ingestion/transactions_v1/mode/000016416000",
                    "ingestion/logs_v1/mode/000016416000",
                    "ingestion/traces_v1/mode/000016416000",
                ],
            ),
            progress_indicator="",
        ),
        IngestionTask(
            chain_max_block=ChainMaxBlock(chain="mode", ts=1736362200, number=18097309),
            requested_max_timestamp=1733097600,
            block_batch=BlockBatch(chain="mode", min=16424000, max=16432000),
            read_from=RawOnchainDataProvider.GOLDSKY,
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
                markers_table="blockbatch_markers",
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
                complete_markers=[
                    "ingestion/blocks_v1/mode/000016424000",
                    "ingestion/transactions_v1/mode/000016424000",
                    "ingestion/logs_v1/mode/000016424000",
                    "ingestion/traces_v1/mode/000016424000",
                ],
            ),
            progress_indicator="",
        ),
        IngestionTask(
            chain_max_block=ChainMaxBlock(chain="mode", ts=1736362200, number=18097309),
            requested_max_timestamp=1733097600,
            block_batch=BlockBatch(chain="mode", min=16432000, max=16440000),
            read_from=RawOnchainDataProvider.GOLDSKY,
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
                markers_table="blockbatch_markers",
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
                complete_markers=[
                    "ingestion/blocks_v1/mode/000016432000",
                    "ingestion/transactions_v1/mode/000016432000",
                    "ingestion/logs_v1/mode/000016432000",
                    "ingestion/traces_v1/mode/000016432000",
                ],
            ),
            progress_indicator="",
        ),
    ]


def test_overlapping_markers():
    candidate_batch = BlockBatch(chain="mode", min=16000, max=24000)

    markers_df = pl.DataFrame(
        [
            # This batch IS NOT OVERLAPPING, it is the preceding batch.
            {
                "root_path": "ingestion/blocks_v1",
                "marker_path": "8000-16000",
                "chain": "mode",
                "min_block": 8000,
                "max_block": 16000,
            },
            # This batch IS NOT OVERLAPPING, it is the same batch.
            {
                "root_path": "ingestion/blocks_v1",
                "marker_path": "16000-24000",
                "chain": "mode",
                "min_block": 16000,
                "max_block": 24000,
            },
            # This batch IS NOT OVERLAPPING, it is the next batch.
            {
                "root_path": "ingestion/blocks_v1",
                "marker_path": "24000-32000",
                "chain": "mode",
                "min_block": 24000,
                "max_block": 32000,
            },
            # This batch IS OVERLAPPING, it has 4000 blocks in common.
            {
                "root_path": "ingestion/blocks_v1",
                "marker_path": "20000-24000",
                "chain": "mode",
                "min_block": 20000,
                "max_block": 24000,
            },
        ]
    )

    actual = overlapping_markers(
        markers_df,
        "ingestion/blocks_v1",
        candidate_batch,
    )

    assert actual == ["20000-24000"]
