from datetime import date
from enum import Enum

import polars as pl

from op_analytics.coreutils.bigquery.gcsexternal import create_gcs_external_table
from op_analytics.coreutils.clickhouse.gcsview import create_gcs_view
from op_analytics.coreutils.clickhouse.inferschema import infer_schema_from_parquet
from op_analytics.coreutils.duckdb_inmem import EmptyParquetData
from op_analytics.coreutils.duckdb_inmem.client import init_client, register_parquet_relation
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import date_tostr

from .dailydataread import make_date_filter, query_parquet_paths
from .dailydataloadch import load_to_clickhouse
from .dailydatawrite import write_daily_data
from .dataaccess import DateFilter
from .location import DataLocation

log = structlog.get_logger()


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
            paths = location.absolute(f"{self.root_path}/dt=*/out.parquet")

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
    def get_all_datasets(cls) -> dict[str, str]:
        """Get all class attributes and their values as a dictionary.

        Returns:
            dict[str, str]: Dictionary mapping dataset names to their values, excluding internal attributes
        """
        return {
            name: value.split(".")[-1]
            for name, value in vars(cls).items()
            if not name.startswith("_") and isinstance(value, str)
        }

    @classmethod
    def infer_all_schemas(cls, datestr: str) -> dict[str, dict]:
        """Infer Clickhouse schemas for all datasets and return with metadata.

        Args:
            datestr: Date string to use for schema inference

        Returns:
            tuple: Tuple containing:
                - datasets: Mapping of class attribute names to values
                - schemas: Mapping of dataset names to their inferred schemas
        """
        schemas = {}
        for dataset in cls.all_tables():
            log.info(f"Inferring schema for {dataset}")
            try:
                schema = dataset.infer_clickhouse_schema(datestr)
                schemas[dataset.value] = schema
            except IndexError:
                log.warning(f"No data found for {dataset} on {datestr}, skipping schema inference")
                continue

        return cls.get_all_datasets(), schemas

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

    def infer_clickhouse_schema(self, datestr: str):
        """Use a single parquet file in GCS to infer the Clickhouse schema for the table."""
        paths = query_parquet_paths(
            self.root_path, DataLocation.GCS, DateFilter.from_dts([datestr])
        )

        infer_schema_from_parquet(paths[0], dummy_name=f"{self.db}.{self.table}")

    def load_gcs_to_ch(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        incremental_overlap: int = 0,
    ):
        """Load dailydata from GCS to clickhouse."""
        try:
            df = self.read_polars(min_date=min_date, max_date=max_date)
        except EmptyParquetData:
            log.info("incremental load: did not find new data")
            return

        summary = load_to_clickhouse(
            db=self.db,
            table=self.table,
            dataframe=df,
            min_date=min_date,
            max_date=max_date,
            incremental_overlap=incremental_overlap,
        )
        return {self.root_path: summary}

    def create_clickhouse_view(self) -> None:
        # Database used in ClickHouse to store views that point to GCS data.
        external_db_name = f"dailydata_{self.db}"

        return create_gcs_view(
            db_name=external_db_name,
            table_name=self.table,
            partition_selection="CAST(dt as Date) AS dt, ",
            gcs_glob_path=f"{self.root_path}/dt=*/out.parquet",
        )

    def create_bigquery_external_table(self) -> None:
        # Database used in BigQuery to store external tables that point to GCS data.
        external_db_name = f"dailydata_{self.db}"

        create_gcs_external_table(
            db_name=external_db_name,
            table_name=self.table,
            partition_columns="dt DATE",
            partition_prefix=self.root_path,
        )
