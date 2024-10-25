import polars as pl
from op_coreutils.storage.paths import (
    KeyValue,
    PartitionedOutput,
    PartitionedPath,
    breakout_partitions,
)


def test_breakout_partitions():
    df = pl.DataFrame(
        {
            "dt": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
            ],
            "chain": ["op", "op", "base", "base", "base", "base", "op"],
            "c": ["some", "words", "here", "and", "some", "more", "blah"],
        }
    )

    outputs = []
    for part_df, part in breakout_partitions(
        df,
        partition_cols=["dt", "chain"],
        root_path="warehouse",
        basename="myfile.parquet",
    ):
        outputs.append(part)
        assert part_df.columns == ["c"]

    outputs.sort(key=lambda x: x.path.full_path)

    assert outputs == [
        PartitionedOutput(
            path=PartitionedPath(
                root="warehouse",
                basename="myfile.parquet",
                partition_cols=[
                    KeyValue(key="dt", value="2024-01-01"),
                    KeyValue(key="chain", value="base"),
                ],
            ),
            row_count=2,
        ),
        PartitionedOutput(
            path=PartitionedPath(
                root="warehouse",
                basename="myfile.parquet",
                partition_cols=[
                    KeyValue(key="dt", value="2024-01-01"),
                    KeyValue(key="chain", value="op"),
                ],
            ),
            row_count=2,
        ),
        PartitionedOutput(
            path=PartitionedPath(
                root="warehouse",
                basename="myfile.parquet",
                partition_cols=[
                    KeyValue(key="dt", value="2024-01-02"),
                    KeyValue(key="chain", value="base"),
                ],
            ),
            row_count=2,
        ),
        PartitionedOutput(
            path=PartitionedPath(
                root="warehouse",
                basename="myfile.parquet",
                partition_cols=[
                    KeyValue(key="dt", value="2024-01-03"),
                    KeyValue(key="chain", value="op"),
                ],
            ),
            row_count=1,
        ),
    ]

    paths = [_.path.full_path for _ in outputs]
    assert paths == [
        "warehouse/dt=2024-01-01/chain=base/myfile.parquet",
        "warehouse/dt=2024-01-01/chain=op/myfile.parquet",
        "warehouse/dt=2024-01-02/chain=base/myfile.parquet",
        "warehouse/dt=2024-01-03/chain=op/myfile.parquet",
    ]
