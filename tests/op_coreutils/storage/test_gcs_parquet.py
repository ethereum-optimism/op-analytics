import polars as pl

from op_coreutils.storage.gcs_parquet import breakout_partitions, PartitionOutput, PartitionCol


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

    outputs.sort()

    assert outputs == [
        PartitionOutput(
            partition=[
                PartitionCol(name="dt", value="2024-01-01"),
                PartitionCol(name="chain", value="base"),
            ],
            path="warehouse/dt=2024-01-01/chain=base/myfile.parquet",
            row_count=2,
        ),
        PartitionOutput(
            partition=[
                PartitionCol(name="dt", value="2024-01-01"),
                PartitionCol(name="chain", value="op"),
            ],
            path="warehouse/dt=2024-01-01/chain=op/myfile.parquet",
            row_count=2,
        ),
        PartitionOutput(
            partition=[
                PartitionCol(name="dt", value="2024-01-02"),
                PartitionCol(name="chain", value="base"),
            ],
            path="warehouse/dt=2024-01-02/chain=base/myfile.parquet",
            row_count=2,
        ),
        PartitionOutput(
            partition=[
                PartitionCol(name="dt", value="2024-01-03"),
                PartitionCol(name="chain", value="op"),
            ],
            path="warehouse/dt=2024-01-03/chain=op/myfile.parquet",
            row_count=1,
        ),
    ]
