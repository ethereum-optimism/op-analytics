import polars as pl
import pytest

from op_analytics.coreutils.partitioned.breakout import breakout_partitions
from op_analytics.coreutils.partitioned.partition import Partition, PartitionColumn, PartitionData
from op_analytics.coreutils.time import date_fromstr

EXPECTED_RESULT = [
    {
        "partitions": Partition(
            cols=[
                PartitionColumn(name="dt", value="2024-01-01"),
                PartitionColumn(name="chain", value="base"),
            ]
        ),
        "full_path": "dt=2024-01-01/chain=base",
        "nu_rows": 2,
    },
    {
        "partitions": Partition(
            cols=[
                PartitionColumn(name="dt", value="2024-01-01"),
                PartitionColumn(name="chain", value="op"),
            ]
        ),
        "full_path": "dt=2024-01-01/chain=op",
        "nu_rows": 2,
    },
    {
        "partitions": Partition(
            cols=[
                PartitionColumn(name="dt", value="2024-01-02"),
                PartitionColumn(name="chain", value="base"),
            ]
        ),
        "full_path": "dt=2024-01-02/chain=base",
        "nu_rows": 2,
    },
    {
        "partitions": Partition(
            cols=[
                PartitionColumn(name="dt", value="2024-01-03"),
                PartitionColumn(name="chain", value="op"),
            ]
        ),
        "full_path": "dt=2024-01-03/chain=op",
        "nu_rows": 1,
    },
]


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
                partitions=_.partition,
                full_path=_.partition.path,
                nu_rows=len(_.df),
            )
        )

    actual.sort(key=lambda x: x["full_path"])
    assert actual == EXPECTED_RESULT


def pass_through(x):
    return x


def test_breakout_partitions_empty():
    for conversion in [pass_through, date_fromstr]:
        df = pl.DataFrame(
            {
                "dt": [
                    conversion("2024-01-01"),
                    conversion("2024-01-01"),
                    conversion("2024-01-01"),
                    conversion("2024-01-01"),
                    conversion("2024-01-02"),
                    conversion("2024-01-02"),
                    conversion("2024-01-03"),
                ],
                "chain": ["op", "op", "base", "base", "base", "base", "op"],
                "c": ["some", "words", "here", "and", "some", "more", "blah"],
            }
        )

        outputs: list[PartitionData] = []
        for part in breakout_partitions(
            df.filter(pl.lit(False)),  # Filter out all data to get a default empty parquet file
            partition_cols=["dt", "chain"],
            default_partitions=[{"chain": "op", "dt": "2023-10-30"}],
        ):
            outputs.append(part)
            assert part.df.columns == ["c"]

        actual = []
        for _ in outputs:
            actual.append(
                dict(
                    partitions=_.partition,
                    full_path=_.partition.path,
                    nu_rows=len(_.df),
                )
            )

        actual.sort(key=lambda x: x["full_path"])
        assert actual == [
            {
                "partitions": Partition(
                    [
                        PartitionColumn(name="chain", value="op"),
                        PartitionColumn(name="dt", value="2023-10-30"),
                    ]
                ),
                "full_path": "chain=op/dt=2023-10-30",
                "nu_rows": 0,
            }
        ]


def test_breakout_partitions_date_column_type():
    df = pl.DataFrame(
        {
            "dt": [
                date_fromstr("2024-01-01"),
                date_fromstr("2024-01-01"),
                date_fromstr("2024-01-01"),
                date_fromstr("2024-01-01"),
                date_fromstr("2024-01-02"),
                date_fromstr("2024-01-02"),
                date_fromstr("2024-01-03"),
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
                partitions=_.partition,
                full_path=_.partition.path,
                nu_rows=len(_.df),
            )
        )

    actual.sort(key=lambda x: x["full_path"])
    assert actual == EXPECTED_RESULT


def test_breakout_partitions_date_column_type_invalid():
    df = pl.DataFrame(
        {
            "dt": [550, 551, 552],
            "chain": ["op", "op", "base"],
            "c": ["some", "words", "here"],
        }
    )

    with pytest.raises(ValueError) as ex:
        list(
            breakout_partitions(
                df,
                partition_cols=["dt", "chain"],
            )
        )

    assert ex.value.args == ("dt partition in dataframe is invalid: must be date or str: 550",)


def test_breakout_partitions_date_value_invalid():
    df = pl.DataFrame(
        {
            "dt": ["180000-01-03", "180000-01-03", "2025-01-01"],
            "chain": ["op", "op", "base"],
            "c": ["some", "words", "here"],
        }
    )

    with pytest.raises(ValueError) as ex:
        list(
            breakout_partitions(
                df,
                partition_cols=["dt", "chain"],
            )
        )

    assert ex.value.args == ("partition value must be a date pattern: '180000-01-03'",)


def test_breakout_partitions_partially_missing_default_partitions():
    for conversion in [pass_through, date_fromstr]:
        df = pl.DataFrame(
            {
                "dt": [
                    conversion("2024-01-01"),
                    conversion("2024-01-01"),
                    conversion("2024-01-01"),
                    conversion("2024-01-01"),
                ],
                "chain": ["op", "op", "op", "op"],
                "c": ["some", "words", "here", "and"],
            }
        )

        outputs: list[PartitionData] = []
        for part in breakout_partitions(
            df,
            partition_cols=["dt", "chain"],
            default_partitions=[
                {"chain": "op", "dt": "2024-01-01"},
                {"chain": "op", "dt": "2024-01-02"},
            ],
        ):
            outputs.append(part)
            assert part.df.columns == ["c"]

        actual = []
        for _ in outputs:
            actual.append(
                dict(
                    partitions=_.partition,
                    full_path=_.partition.path,
                    num_rows=len(_.df),
                )
            )

        actual.sort(key=lambda x: x["full_path"])
        assert actual == [
            {
                "partitions": Partition(
                    cols=[
                        PartitionColumn(name="dt", value="2024-01-01"),
                        PartitionColumn(name="chain", value="op"),
                    ]
                ),
                "full_path": "dt=2024-01-01/chain=op",
                "num_rows": 4,
            },
            {
                "partitions": Partition(
                    cols=[
                        PartitionColumn(name="dt", value="2024-01-02"),
                        PartitionColumn(name="chain", value="op"),
                    ]
                ),
                "full_path": "dt=2024-01-02/chain=op",
                "num_rows": 0,
            },
        ]
