from op_datasets.blockrange import BlockRange
from op_datasets.processing.ozone import UnitOfWork, determine_tasks


def test_path_01():
    br = BlockRange.from_spec("245156:+15000")
    actual = UnitOfWork(chain="op", dt="2024-10-03", block_range=br).construct_path("blocks")
    assert actual == "blocks/chain=op/dt=2024-10-03/000000240000__245156-260156.parquet"


def test_tasks_01():
    br = BlockRange.from_spec("245156:+15000")
    tasks = determine_tasks(br)
    assert tasks == [
        BlockRange(min=245156, max=250000),
        BlockRange(min=250000, max=260000),
        BlockRange(min=260000, max=260156),
    ]
