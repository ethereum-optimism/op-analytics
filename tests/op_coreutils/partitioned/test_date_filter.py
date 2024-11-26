import datetime

from op_analytics.coreutils.partitioned.dataaccess import DateFilter
from op_analytics.datapipeline.utils.daterange import DateRange


def test01():
    df = DateFilter(
        min_date=None,
        max_date=None,
        datevals=DateRange.from_spec("@20241001:+5").dates,
    )

    ch_where, ch_parameters = df.sql_clickhouse()
    assert ch_where == "dt IN {dates:Array(Date)}"
    assert ch_parameters == {
        "dates": [
            datetime.date(2024, 10, 1),
            datetime.date(2024, 10, 2),
            datetime.date(2024, 10, 3),
            datetime.date(2024, 10, 4),
            datetime.date(2024, 10, 5),
        ]
    }

    db_where = df.sql_duckdb()
    assert (
        db_where == "dt IN ('2024-10-01', '2024-10-02', '2024-10-03', '2024-10-04', '2024-10-05')"
    )


def test02():
    df = DateFilter(
        min_date=datetime.date(2020, 1, 1),
        max_date=None,
        datevals=None,
    )

    ch_where, ch_parameters = df.sql_clickhouse()
    assert ch_where == "dt >= {mindate:Date}"
    assert ch_parameters == {"mindate": datetime.date(2020, 1, 1)}

    db_where = df.sql_duckdb()
    assert db_where == "dt >= '2020-01-01'"


def test03():
    df = DateFilter(
        min_date=datetime.date(2020, 1, 1),
        max_date=datetime.date(2021, 1, 1),
        datevals=None,
    )

    ch_where, ch_parameters = df.sql_clickhouse()
    assert ch_where == "dt >= {mindate:Date} AND dt < {maxdate:Date}"
    assert ch_parameters == {
        "maxdate": datetime.date(2021, 1, 1),
        "mindate": datetime.date(2020, 1, 1),
    }

    db_where = df.sql_duckdb()
    assert db_where == "dt >= '2020-01-01' AND dt < '2021-01-01'"


def test04():
    df = DateFilter(
        min_date=None,
        max_date=None,
        datevals=None,
    )

    ch_where, ch_parameters = df.sql_clickhouse()
    assert ch_where == ""
    assert ch_parameters == {}

    db_where = df.sql_duckdb()
    assert db_where == ""

    assert df.is_undefined
