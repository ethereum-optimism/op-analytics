import pytest

from op_datasets.processing.blockrange import BlockRange
from op_datasets.processing.ozone import (
    BatchOutputs,
    BlockBatch,
    Delimiter,
    find_batch_delimiter,
    split_block_range,
    split_block_range_from_boundaries,
    InvalidMicrobatchConfig,
)


def test_path_01():
    br = BlockRange.from_spec("245156:+15000")
    task = split_block_range(chain="op", block_range=br)[0]

    actual = BatchOutputs(dt="2024-10-03", block_batch=task).construct_path("blocks")
    assert actual == "blocks/chain=op/dt=2024-10-03/000000244000.parquet"


def test_batches_01():
    br = BlockRange.from_spec("245156:+15000")
    tasks = split_block_range(chain="op", block_range=br)
    assert tasks == [
        BlockBatch(chain="op", min=244000, max=246000),
        BlockBatch(chain="op", min=246000, max=248000),
        BlockBatch(chain="op", min=248000, max=250000),
        BlockBatch(chain="op", min=250000, max=252000),
        BlockBatch(chain="op", min=252000, max=254000),
        BlockBatch(chain="op", min=254000, max=256000),
        BlockBatch(chain="op", min=256000, max=258000),
        BlockBatch(chain="op", min=258000, max=260000),
        BlockBatch(chain="op", min=260000, max=262000),
    ]


def test_batches_02():
    br = BlockRange.from_spec("10:+2000")
    boundaries = [
        Delimiter(block_number=0, batch_size=100),
        Delimiter(600, 200),
        Delimiter(1200, 400),
        Delimiter(2000, 500),
    ]

    tasks = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)
    assert tasks == [
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

    tasks = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)
    assert tasks == [
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

    tasks = split_block_range_from_boundaries(chain="op", boundaries=boundaries, block_range=br)
    assert tasks == [
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
