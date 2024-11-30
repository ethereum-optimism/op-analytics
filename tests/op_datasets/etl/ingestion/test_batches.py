import pytest

from op_analytics.coreutils.partitioned import DataLocation
from op_analytics.datapipeline.etl.ingestion.batches import (
    BlockBatch,
    Delimiter,
    InvalidMicrobatchConfig,
    find_batch_delimiter,
    split_block_range,
    split_block_range_from_boundaries,
)
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider
from op_analytics.datapipeline.etl.ingestion.task import IngestionTask
from op_analytics.datapipeline.utils.blockrange import BlockRange


def test_batches_01():
    br = BlockRange.from_spec("245156:+15000")
    batches = split_block_range(chain="op", block_range=br)
    assert batches == [
        BlockBatch(chain="op", min=240000, max=250000),
        BlockBatch(chain="op", min=250000, max=260000),
        BlockBatch(chain="op", min=260000, max=270000),
    ]


def test_batches_02():
    br = BlockRange.from_spec("10:+2000")
    boundaries = [
        Delimiter(block_number=0, batch_size=100),
        Delimiter(600, 200),
        Delimiter(1200, 400),
        Delimiter(2000, 500),
    ]

    batches = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)
    assert batches == [
        BlockBatch(chain="op", min=0, max=100),
        BlockBatch(chain="op", min=100, max=200),
        BlockBatch(chain="op", min=200, max=300),
        BlockBatch(chain="op", min=300, max=400),
        BlockBatch(chain="op", min=400, max=500),
        BlockBatch(chain="op", min=500, max=600),
        BlockBatch(chain="op", min=600, max=800),
        BlockBatch(chain="op", min=800, max=1000),
        BlockBatch(chain="op", min=1000, max=1200),
        BlockBatch(chain="op", min=1200, max=1600),
        BlockBatch(chain="op", min=1600, max=2000),
        BlockBatch(chain="op", min=2000, max=2500),
    ]


def test_batches_03():
    br = BlockRange.from_spec("210:+2600")
    boundaries = [
        Delimiter(block_number=0, batch_size=200),
        Delimiter(1200, 400),
        Delimiter(2000, 500),
    ]

    batches = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)
    assert batches == [
        BlockBatch(chain="op", min=200, max=400),
        BlockBatch(chain="op", min=400, max=600),
        BlockBatch(chain="op", min=600, max=800),
        BlockBatch(chain="op", min=800, max=1000),
        BlockBatch(chain="op", min=1000, max=1200),
        BlockBatch(chain="op", min=1200, max=1600),
        BlockBatch(chain="op", min=1600, max=2000),
        BlockBatch(chain="op", min=2000, max=2500),
        BlockBatch(chain="op", min=2500, max=3000),
    ]


def test_batches_04():
    br = BlockRange.from_spec("210:+2600")
    boundaries = boundaries = [
        Delimiter(block_number=0, batch_size=800),
        Delimiter(1600, 400),
        Delimiter(2800, 200),
    ]

    batches = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)
    assert batches == [
        BlockBatch(chain="op", min=0, max=800),
        BlockBatch(chain="op", min=800, max=1600),
        BlockBatch(chain="op", min=1600, max=2000),
        BlockBatch(chain="op", min=2000, max=2400),
        BlockBatch(chain="op", min=2400, max=2800),
        BlockBatch(chain="op", min=2800, max=3000),
    ]


def test_batches_error():
    br = BlockRange.from_spec("210:+2600")
    boundaries = [
        Delimiter(block_number=0, batch_size=200),
        Delimiter(1200, 400),
        Delimiter(2100, 500),
    ]

    with pytest.raises(InvalidMicrobatchConfig) as ex:
        split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)

    assert ex.value.args == (
        "delimiter block number should align: Delimiter(block_number=1200, batch_size=400) -> Delimiter(block_number=2100, batch_size=500)",
    )


def test_find_batch_start_01():
    boundaries = [
        Delimiter(block_number=0, batch_size=100),
        Delimiter(600, 200),
        Delimiter(1200, 400),
        Delimiter(2000, 500),
    ]

    def actual(_nbr):
        return find_batch_delimiter(boundaries, _nbr)

    assert actual(50) == Delimiter(block_number=0, batch_size=100)
    assert actual(599) == Delimiter(block_number=500, batch_size=100)
    assert actual(600) == Delimiter(block_number=600, batch_size=200)
    assert actual(715) == Delimiter(block_number=600, batch_size=200)
    assert actual(1115) == Delimiter(block_number=1000, batch_size=200)
    assert actual(1199) == Delimiter(block_number=1000, batch_size=200)
    assert actual(1200) == Delimiter(block_number=1200, batch_size=400)
    assert actual(1205) == Delimiter(block_number=1200, batch_size=400)
    assert actual(2100) == Delimiter(block_number=2000, batch_size=500)


def test_find_batch_start_02():
    boundaries = [
        Delimiter(block_number=0, batch_size=800),
        Delimiter(1600, 400),
        Delimiter(2800, 200),
    ]

    def actual(_nbr):
        return find_batch_delimiter(boundaries, _nbr)

    assert actual(50) == Delimiter(block_number=0, batch_size=800)
    assert actual(715) == Delimiter(block_number=0, batch_size=800)
    assert actual(1115) == Delimiter(block_number=800, batch_size=800)
    assert actual(1205) == Delimiter(block_number=800, batch_size=800)
    assert actual(1700) == Delimiter(block_number=1600, batch_size=400)
    assert actual(2800) == Delimiter(block_number=2800, batch_size=200)
    assert actual(1600) == Delimiter(block_number=1600, batch_size=400)


def test_expected_markers():
    br = BlockRange.from_spec("210:+2600")
    boundaries = boundaries = [
        Delimiter(block_number=0, batch_size=800),
        Delimiter(1600, 400),
        Delimiter(2800, 200),
    ]

    batches = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)

    task = IngestionTask.new(
        max_requested_timestamp=None,
        block_batch=batches[0],
        read_from=RawOnchainDataProvider.GOLDSKY,
        write_to=DataLocation.DISABLED,
    )

    actual = [
        dict(
            dataset_name=_.dataset_name,
            marker_path=_.marker_path,
            additional_columns=_.additional_columns,
        )
        for _ in task.data_writer.expected_outputs
    ]

    assert actual == [
        {
            "dataset_name": "blocks",
            "marker_path": "markers/ingestion/blocks_v1/chain=op/000000000000.json",
            "additional_columns": {"num_blocks": 800, "min_block": 0, "max_block": 800},
        },
        {
            "dataset_name": "transactions",
            "marker_path": "markers/ingestion/transactions_v1/chain=op/000000000000.json",
            "additional_columns": {"num_blocks": 800, "min_block": 0, "max_block": 800},
        },
        {
            "dataset_name": "logs",
            "marker_path": "markers/ingestion/logs_v1/chain=op/000000000000.json",
            "additional_columns": {"num_blocks": 800, "min_block": 0, "max_block": 800},
        },
        {
            "dataset_name": "traces",
            "marker_path": "markers/ingestion/traces_v1/chain=op/000000000000.json",
            "additional_columns": {"num_blocks": 800, "min_block": 0, "max_block": 800},
        },
    ]


def test_batches_base():
    # Switching over from 2k -> 1k blocks per batch.
    br = BlockRange.from_spec("20894000:20904000")

    batches = split_block_range(chain="base", block_range=br)
    assert batches == [
        BlockBatch(chain="base", min=20894000, max=20896000),
        BlockBatch(chain="base", min=20896000, max=20898000),
        BlockBatch(chain="base", min=20898000, max=20900000),
        # Here it goes from 2000 -> 1000 blocks per batch
        BlockBatch(chain="base", min=20900000, max=20901000),
        BlockBatch(chain="base", min=20901000, max=20902000),
        BlockBatch(chain="base", min=20902000, max=20903000),
        BlockBatch(chain="base", min=20903000, max=20904000),
    ]

    # Switching over from 1k -> 400 blocks per batch.
    br = BlockRange.from_spec("21189000:21201000")
    batches = split_block_range(chain="base", block_range=br)
    assert batches == [
        BlockBatch(chain="base", min=21189000, max=21190000),
        BlockBatch(chain="base", min=21190000, max=21191000),
        BlockBatch(chain="base", min=21191000, max=21192000),
        BlockBatch(chain="base", min=21192000, max=21193000),
        BlockBatch(chain="base", min=21193000, max=21194000),
        BlockBatch(chain="base", min=21194000, max=21195000),
        BlockBatch(chain="base", min=21195000, max=21196000),
        BlockBatch(chain="base", min=21196000, max=21197000),
        BlockBatch(chain="base", min=21197000, max=21198000),
        BlockBatch(chain="base", min=21198000, max=21199000),
        BlockBatch(chain="base", min=21199000, max=21200000),
        # Here it goes from 1000 -> 400 blocks per batch
        BlockBatch(chain="base", min=21200000, max=21200400),
        BlockBatch(chain="base", min=21200400, max=21200800),
        BlockBatch(chain="base", min=21200800, max=21201200),
    ]
