import polars as pl


from op_analytics.coreutils.partitioned.breakout import breakout_partitions
from op_analytics.coreutils.partitioned.output import (
    PartitionData,
    OutputPartMeta,
    PartitionColumn,
    PartitionColumns,
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

    outputs: list[PartitionData] = []
    for part in breakout_partitions(
        df,
        partition_cols=["dt", "chain"],
    ):
        outputs.append(part)
        assert part.df.columns == ["c"]

    actual = []
    for _ in outputs:
        actual.append(
            dict(
                meta=_.meta,
                full_path=_.meta.partitions.path,
                nu_rows=len(_.df),
            )
        )

    actual.sort(key=lambda x: x["meta"].partitions.path)
    assert actual == [
        {
            "meta": OutputPartMeta(
                partitions=PartitionColumns(
                    [
                        PartitionColumn(name="dt", value="2024-01-01"),
                        PartitionColumn(name="chain", value="base"),
                    ]
                ),
                row_count=2,
            ),
            "full_path": "dt=2024-01-01/chain=base",
            "nu_rows": 2,
        },
        {
            "meta": OutputPartMeta(
                partitions=PartitionColumns(
                    [
                        PartitionColumn(name="dt", value="2024-01-01"),
                        PartitionColumn(name="chain", value="op"),
                    ]
                ),
                row_count=2,
            ),
            "full_path": "dt=2024-01-01/chain=op",
            "nu_rows": 2,
        },
        {
            "meta": OutputPartMeta(
                partitions=PartitionColumns(
                    [
                        PartitionColumn(name="dt", value="2024-01-02"),
                        PartitionColumn(name="chain", value="base"),
                    ]
                ),
                row_count=2,
            ),
            "full_path": "dt=2024-01-02/chain=base",
            "nu_rows": 2,
        },
        {
            "meta": OutputPartMeta(
                partitions=PartitionColumns(
                    [
                        PartitionColumn(name="dt", value="2024-01-03"),
                        PartitionColumn(name="chain", value="op"),
                    ]
                ),
                row_count=1,
            ),
            "full_path": "dt=2024-01-03/chain=op",
            "nu_rows": 1,
        },
    ]


def test_breakout_partitions_empty():
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

    outputs: list[PartitionData] = []
    for part in breakout_partitions(
        df.filter(pl.col("dt") == ""),  # Filter out all data to get a default empty parquet file
        partition_cols=["dt", "chain"],
        default_partition={"chain": "op", "dt": "2023-10-30"},
    ):
        outputs.append(part)
        assert part.df.columns == ["c"]

    actual = []
    for _ in outputs:
        actual.append(
            dict(
                meta=_.meta,
                full_path=_.meta.partitions.path,
                nu_rows=len(_.df),
            )
        )

    actual.sort(key=lambda x: x["meta"].partitions.path)
    assert actual == [
        {
            "meta": OutputPartMeta(
                partitions=PartitionColumns(
                    [
                        PartitionColumn(name="chain", value="op"),
                        PartitionColumn(name="dt", value="2023-10-30"),
                    ]
                ),
                row_count=0,
            ),
            "full_path": "chain=op/dt=2023-10-30",
            "nu_rows": 0,
        }
    ]
