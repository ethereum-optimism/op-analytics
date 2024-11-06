import polars as pl


from op_coreutils.partitioned.breakout import breakout_partitions
from op_coreutils.partitioned.output import OutputPart, OutputPartMeta, KeyValue


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

    outputs: list[OutputPart] = []
    for part in breakout_partitions(
        df,
        partition_cols=["dt", "chain"],
    ):
        outputs.append(part)
        assert part.df.columns == ["c"]

    actual = sorted([_.meta for _ in outputs], key=lambda x: x.partitions_path)

    assert actual == [
        OutputPartMeta(
            partitions=[
                KeyValue(key="dt", value="2024-01-01"),
                KeyValue(key="chain", value="base"),
            ],
            row_count=2,
        ),
        OutputPartMeta(
            partitions=[
                KeyValue(key="dt", value="2024-01-01"),
                KeyValue(key="chain", value="op"),
            ],
            row_count=2,
        ),
        OutputPartMeta(
            partitions=[
                KeyValue(key="dt", value="2024-01-02"),
                KeyValue(key="chain", value="base"),
            ],
            row_count=2,
        ),
        OutputPartMeta(
            partitions=[
                KeyValue(key="dt", value="2024-01-03"),
                KeyValue(key="chain", value="op"),
            ],
            row_count=1,
        ),
    ]

    paths = sorted([_.meta.full_path("warehouse", "myfile.parquet") for _ in outputs])
    assert paths == [
        "warehouse/dt=2024-01-01/chain=base/myfile.parquet",
        "warehouse/dt=2024-01-01/chain=op/myfile.parquet",
        "warehouse/dt=2024-01-02/chain=base/myfile.parquet",
        "warehouse/dt=2024-01-03/chain=op/myfile.parquet",
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

    outputs: list[OutputPart] = []
    for part in breakout_partitions(
        df.filter(pl.col("dt") == ""),  # Filter out all data to get a default empty parquet file
        partition_cols=["dt", "chain"],
        default_partition={"chain": "op", "dt": "2023-10-30"},
    ):
        outputs.append(part)
        assert part.df.columns == ["c"]

    actual = sorted([_.meta for _ in outputs], key=lambda x: x.partitions_path)

    assert actual == [
        OutputPartMeta(
            partitions=[KeyValue(key="chain", value="op"), KeyValue(key="dt", value="2023-10-30")],
            row_count=0,
        )
    ]

    paths = [_.meta.full_path("warehouse", "myfile.parquet") for _ in outputs]
    assert paths == ["warehouse/chain=op/dt=2023-10-30/myfile.parquet"]
