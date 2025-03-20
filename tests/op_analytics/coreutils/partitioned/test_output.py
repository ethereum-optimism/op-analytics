from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import PartitionColumn, Partition


def test_01():
    expected_output = ExpectedOutput(
        root_path="model/rootpath",
        file_name="out.parquet",
        marker_path="BLAH",
    )

    partition = Partition(
        [
            PartitionColumn(name="chain", value="op"),
            PartitionColumn(name="dt", value="2023-10-30"),
        ]
    )
    actual = partition.full_path(expected_output.root_path, expected_output.file_name)

    assert actual == "model/rootpath/chain=op/dt=2023-10-30/out.parquet"
