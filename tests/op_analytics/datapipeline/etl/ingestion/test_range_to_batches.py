import pytest

from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.datapipeline.etl.ingestion.batches import (
    BlockBatch,
    Delimiter,
    InvalidMicrobatchConfig,
    split_block_range,
    split_block_range_from_boundaries,
)


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
