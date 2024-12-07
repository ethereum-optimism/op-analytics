import pyarrow as pa

from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import PartitionColumn, Partition


def test_01():
    expected_output = ExpectedOutput(
        root_path="intermediate/daily_address_summary/summary_v1",
        file_name="out.parquet",
        marker_path="BLAH",
        process_name="default",
        additional_columns={"model_name": "MYMODEL"},
        additional_columns_schema=[
            pa.field("chain", pa.string()),
            pa.field("dt", pa.date32()),
            pa.field("model_name", pa.string()),
        ],
    )

    partition = Partition(
        [
            PartitionColumn(name="chain", value="op"),
            PartitionColumn(name="dt", value="2023-10-30"),
        ]
    )
    actual = partition.full_path(expected_output.root_path, expected_output.file_name)

    assert (
        actual == "intermediate/daily_address_summary/summary_v1/chain=op/dt=2023-10-30/out.parquet"
    )
