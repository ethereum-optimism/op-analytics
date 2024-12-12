from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import date_fromstr


log = structlog.get_logger()


@dataclass
class MarkerFilter:
    column: str
    values: list[str]

    def clickhouse_filter(self, param: str):
        return " AND %s in {%s:Array(String)}" % (self.column, param)


@dataclass
class DateFilter:
    min_date: date | None
    max_date: date | None
    datevals: list[date] | None

    @classmethod
    def from_dts(cls, datestrs: list[str]):
        return DateFilter(
            min_date=None,
            max_date=None,
            datevals=[date_fromstr(_) for _ in datestrs],
        )

    @property
    def is_undefined(self):
        return all(
            [
                self.min_date is None,
                self.max_date is None,
                self.datevals is None,
            ]
        )

    def sql_clickhouse(self):
        where = []
        parameters: dict[str, Any] = {}

        if self.datevals is not None and len(self.datevals) > 0:
            where.append("dt IN {dates:Array(Date)}")
            parameters["dates"] = self.datevals

        if self.min_date is not None:
            where.append("dt >= {mindate:Date}")
            parameters["mindate"] = self.min_date

        if self.max_date is not None:
            where.append("dt < {maxdate:Date}")
            parameters["maxdate"] = self.max_date

        return " AND ".join(where), parameters

    def sql_duckdb(self):
        where = []
        if self.datevals is not None and len(self.datevals) > 0:
            datelist = ", ".join([f"'{_.strftime("%Y-%m-%d")}'" for _ in self.datevals])
            where.append(f"dt IN ({datelist})")

        if self.min_date is not None:
            where.append(f"dt >= '{self.min_date.strftime("%Y-%m-%d")}'")

        if self.max_date is not None:
            where.append(f"dt < '{self.max_date.strftime("%Y-%m-%d")}'")

        return " AND ".join(where)


class MarkerStore(Protocol):
    def write_marker(
        self,
        markers_table: str,
        marker_df: pa.Table,
    ) -> None: ...

    def query_single_marker(
        self,
        marker_path: str,
        markers_table: str,
    ) -> pl.DataFrame: ...

    def query_markers(
        self,
        markers_table: str,
        datefilter: DateFilter,
        projections=list[str],
        filters=dict[str, MarkerFilter],
    ) -> pl.DataFrame: ...
