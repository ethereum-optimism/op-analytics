from dataclasses import dataclass
from datetime import date
from enum import Enum

import polars as pl

from op_analytics.coreutils.bigquery.gcsexternal import create_gcs_external_table
from op_analytics.coreutils.duckdb_inmem.client import init_client, register_parquet_relation
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import date_tostr

from .dailydataread import make_date_filter, query_parquet_paths
from .dailydatawrite import PARQUET_FILENAME, write_daily_data
from .dataaccess import DateFilter
from .location import DataLocation

log = structlog.get_logger()


# We use the default "dt" value for cases when we run an ingestion process daily
# but only care about storing the most recently pulled data.
DEFAULT_DT = "2000-01-01"


@dataclass
class TablePath:
    db: str
    table: str


class DailyDataset(str, Enum):
    """Base class for daily datasets.

    The name of the subclass is the name of the dataset and the enum values
    are names of the tables that are part of the dataset.

    See for example: DefiLlama, GrowThePie
    """

    @classmethod
    def all_tables(cls) -> list["DailyDataset"]:
        return list(cls.__members__.values())

    @property
    def db(self):
        return self.__class__.__name__.lower()

    @property
    def table(self):
        return self.value

    @property
    def root_path(self):
        return f"{self.db}/{self.table}"

    def write(
        self,
        dataframe: pl.DataFrame,
        sort_by: list[str] | None = None,
    ):
        return write_daily_data(
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
        )

    def read(
        self,
        min_date: str | date | None = None,
        max_date: str | date | None = None,
        date_range_spec: str | None = None,
        location: DataLocation = DataLocation.GCS,
    ) -> str:
        """Load date partitioned defillama dataset from the specified location.

        The loaded data is registered as duckdb view.

        The name of the registered view is returned.
        """
        if isinstance(min_date, date):
            min_date = date_tostr(min_date)

        if isinstance(max_date, date):
            max_date = date_tostr(max_date)

        log.info(
            f"Reading data from {self.root_path!r} "
            f"with filters min_date={min_date}, max_date={max_date}, date_range_spec={date_range_spec}"
        )

        duckdb_context = init_client()

        datefilter = make_date_filter(min_date, max_date, date_range_spec)

        paths: str | list[str]
        if datefilter.is_undefined:
            paths = location.absolute(f"{self.root_path}/dt=*/{PARQUET_FILENAME}")

        else:
            paths = query_parquet_paths(
                root_path=self.root_path,
                location=location,
                datefilter=datefilter,
            )

        if not paths:
            raise Exception(f"Did not find parquet paths for date filter: {datefilter}")

        view_name = register_parquet_relation(dataset=self.root_path, parquet_paths=paths)
        print(duckdb_context.client.sql("SHOW TABLES"))
        return view_name

    @classmethod
    def infer_all_schemas(cls, datestr: str):
        """Infer parquet schemas for all datasets and return with metadata.

        This function can be used for ad-hoc analytics & schema inference,
        user facing data discoverability tools and data validation and testing.

        Args:
            datestr: Date string to use for schema inference

        Returns:
            dict[DailyDataset, dict]: Mapping of dataset enum members to their inferred schemas
        """
        schemas = {}
        for table in cls.all_tables():
            log.info(f"Inferring schema for {table.name}")
            try:
                # Find paths for this root path.
                paths = query_parquet_paths(
                    table.root_path, DataLocation.GCS, DateFilter.from_dts([datestr])
                )

                # Read the first path using polars to get the schema.
                sample_parquet_path = paths[0]
                schema = [
                    {"name": col, "type": str(dtype).upper()}
                    for col, dtype in pl.scan_parquet(sample_parquet_path).collect_schema().items()
                ]

                # Map the table to it's schema.
                schemas[table] = schema

            except IndexError:
                log.warning(
                    f"No data found for {table.name} on {datestr}, skipping schema inference"
                )
                continue

        return schemas

    def read_polars(
        self,
        min_date: str | date | None = None,
        max_date: str | date | None = None,
        date_range_spec: str | None = None,
        location: DataLocation = DataLocation.GCS,
    ) -> pl.DataFrame:
        duckdb_context = init_client()

        relation_name = self.read(
            min_date=min_date,
            max_date=max_date,
            date_range_spec=date_range_spec,
            location=location,
        )
        rel = duckdb_context.client.sql(f"SELECT * FROM {relation_name}")
        return rel.pl()

    def create_bigquery_external_table(self) -> None:
        # Database used in BigQuery to store external tables that point to GCS data.
        external_db_name = f"dailydata_{self.db}"

        create_gcs_external_table(
            db_name=external_db_name,
            table_name=self.table,
            partition_columns="dt DATE",
            partition_prefix=self.root_path,
        )

    def clickhouse_buffer_table(self) -> TablePath:
        """Return db and name for the buffer table in ClickHouse.

        We use buffer tables in ClickHouse as a way of doing "streaming inserts",
        where we insert data row by row or in smaller chunks as a way to save
        progress on data pulls that ingest data from many small invididual request.

        The primary use case is DefiLLama, where we have to fetch many individual
        endpoints (each protocol or each stablecoin).

        We standardize the location of buffer tables. However we don't standardize
        the DDL used to create buffer tables as schemas can be depend on the use case.

        Users are responsible for manually creating the buffer tables on ClickHouse.
        """
        return TablePath(db="datapullbuffer", table=f"{self.db}_{self.table}")
