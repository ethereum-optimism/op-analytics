from op_datasets.processing.blockrange import BlockRange
from op_datasets.processing.ozone import DateTask, split_block_range


def test_path_01():
    br = BlockRange.from_spec("245156:+15000")
    actual = DateTask(chain="op", dt="2024-10-03", block_range=br).construct_path("blocks")
    assert actual == "blocks/chain=op/dt=2024-10-03/000000244000.parquet"


def test_tasks_01():
    br = BlockRange.from_spec("245156:+15000")
    tasks = split_block_range(br)
    assert tasks == [
        BlockRange(min=244000, max=246000),
        BlockRange(min=246000, max=248000),
        BlockRange(min=248000, max=250000),
        BlockRange(min=250000, max=252000),
        BlockRange(min=252000, max=254000),
        BlockRange(min=254000, max=256000),
        BlockRange(min=256000, max=258000),
        BlockRange(min=258000, max=260000),
        BlockRange(min=260000, max=262000),
    ]
