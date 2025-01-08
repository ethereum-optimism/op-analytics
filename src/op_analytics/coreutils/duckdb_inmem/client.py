import os
import tempfile
from dataclasses import dataclass
from threading import Lock

import duckdb
import polars as pl
import pyarrow as pa
from overrides import EnforceOverrides, override

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog, human_size

log = structlog.get_logger()


@dataclass
class DuckDBContext:
    client: duckdb.DuckDBPyConnection

    # Path to the temporary directory where the duckdb database file is stored.
    dir_name: str

    # Database file name
    db_file_name: str

    # Flag that keeps track of python udf registration.
    python_udfs_ready: bool = False

    @property
    def db_path(self):
        return os.path.join(self.dir_name, self.db_file_name)

    def report_size(self):
        size = os.path.getsize(self.db_path)
        log.info(f"duck db size: {human_size(size)}")

    def close(self):
        self.client.close()
        # shutil.rmtree(self.dir_name)

    def make_path(self, file_name: str) -> str:
        return os.path.join(self.dir_name, file_name)

    def connect_to_gcs(self):
        self.client.sql("INSTALL httpfs")
        KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
        SECRET = env_get("GCS_HMAC_SECRET")
        self.client.sql(f"""
        CREATE SECRET (
            TYPE GCS,
            KEY_ID '{KEY_ID}',
            SECRET '{SECRET}'
        );
        """)

    def relation_to_polars(self, rel: duckdb.DuckDBPyRelation | str) -> pl.DataFrame:
        if isinstance(rel, duckdb.DuckDBPyRelation):
            return rel.pl()

        if isinstance(rel, str):
            return self.client.sql(f"SELECT * FROM {rel}").pl()

        raise NotImplementedError()

    def relation_to_arrow(self, rel: duckdb.DuckDBPyRelation | str) -> pa.Table:
        if isinstance(rel, duckdb.DuckDBPyRelation):
            return rel.arrow()

        if isinstance(rel, str):
            return self.client.sql(f"SELECT * FROM {rel}").arrow()

        raise NotImplementedError()


_DUCK_DB: DuckDBContext | None = None

_INIT_LOCK = Lock()


def init_client() -> DuckDBContext:
    global _DUCK_DB

    with _INIT_LOCK:
        if _DUCK_DB is None:
            dirname = tempfile.mkdtemp(dir=os.environ.get("DUCKDB_DATADIR"), prefix="")
            filename = "op-analytics.duck.db"
            _DUCK_DB = DuckDBContext(
                client=duckdb.connect(os.path.join(dirname, filename)),
                dir_name=dirname,
                db_file_name=filename,
            )

            log.info(f"initialized duckdb at {_DUCK_DB.db_path}")

            # Setup access to GCS
            _DUCK_DB.connect_to_gcs()

    if _DUCK_DB is None:
        raise RuntimeError("DuckDB client was not properly initialized.")

    return _DUCK_DB


def sanitized_table_name(dataset_name: str) -> str:
    return dataset_name.replace("/", "_")


class EmptyParquetData(Exception):
    pass


@dataclass
class CreateStatement:
    """A statement that creates a table or view.

    The name of the created table is available in the `name` attribute. This is so it
    can be used in SQL queries that refer to the created object.
    """

    # Name of the TABLE or VIEW that gets created by this statement.
    name: str

    # CREATE TABLE or CREATE VIEW statement.
    sql: str


class ParquetData(EnforceOverrides):
    sanitized_name: str

    def duckdb_ctx(self) -> DuckDBContext:
        raise NotImplementedError()

    def data_subquery(self) -> str:
        raise NotImplementedError()

    def select_string(
        self,
        projections: list[str] | None = None,
        additional_sql: str | None = None,
        parenthesis: bool = False,
    ) -> str:
        select = f"""
        SELECT {projections or "*"} FROM {self.data_subquery()}
        {additional_sql or ""}
        """

        return f"(\n{select}\n)" if parenthesis else select

    def as_subquery(
        self,
        projections: list[str] | None = None,
        additional_sql: str | None = None,
    ):
        return self.select_string(
            projections=projections, additional_sql=additional_sql, parenthesis=True
        )

    def create_table_statement(
        self, projections: list[str] | None = None, additional_sql: str | None = None
    ) -> CreateStatement:
        name = f"{self.sanitized_name}_tbl"

        return CreateStatement(
            name=name,
            sql=f"""
            CREATE OR REPLACE TABLE {name} AS
            {self.select_string(projections, additional_sql)};
            """,
        )

    def create_view_statement(
        self, projections: list[str] | None = None, additional_sql: str | None = None
    ) -> CreateStatement:
        name = f"{self.sanitized_name}_view"

        return CreateStatement(
            name=name,
            sql=f"""
            CREATE OR REPLACE VIEW {name} AS
            {self.select_string(projections, additional_sql)};
            """,
        )

    def execute_create(self, statement: CreateStatement) -> str:
        ctx = self.duckdb_ctx()

        ctx.client.sql(statement.sql)
        log.info(f"created table/view {statement.name}")
        ctx.report_size()
        return statement.name

    def create_table(
        self, projections: list[str] | None = None, additional_sql: str | None = None
    ) -> str:
        statement = self.create_table_statement(
            projections=projections, additional_sql=additional_sql
        )
        return self.execute_create(statement)

    def create_view(
        self, projections: list[str] | None = None, additional_sql: str | None = None
    ) -> str:
        statement = self.create_view_statement(
            projections=projections, additional_sql=additional_sql
        )
        return self.execute_create(statement)


@dataclass
class RemoteParquetData(ParquetData):
    sanitized_name: str
    paths: list[str]

    @override
    def duckdb_ctx(self) -> DuckDBContext:
        return init_client()

    @override
    def data_subquery(self) -> str:
        return self.read_parquet_string()

    @classmethod
    def for_dataset(cls, dataset: str, parquet_paths: list[str] | str) -> "RemoteParquetData":
        if not parquet_paths:
            raise Exception("cannot initalize RemoteParquetData with empty parquet_paths")
        return cls(
            sanitized_name=sanitized_table_name(dataset),
            paths=parquet_paths if isinstance(parquet_paths, list) else [parquet_paths],
        )

    def __post_init__(self):
        if len(self.paths) == 0:
            raise EmptyParquetData()

    @property
    def num_paths(self):
        return len(self.paths)

    def read_parquet_string(self) -> str:
        """Return escaped paths as a single string that can be interpolated into SQL"""

        paths_str = ",\n".join(f"'{_}'" for _ in self.paths)

        log.info(f"constructed read_parquet() string with {self.num_paths} paths")

        return f"""
        read_parquet(
            [
                {paths_str}
            ],
            hive_partitioning = true
        )
        """


def register_parquet_relation(dataset: str, parquet_paths: list[str] | str) -> str:
    """Return a DuckDB relation from a list of parquet files."""
    ctx = init_client()
    client = ctx.client

    if not parquet_paths:
        raise EmptyParquetData()

    rel = client.read_parquet(parquet_paths, hive_partitioning=True)  # type: ignore

    if isinstance(parquet_paths, str):
        summary = f"using uri wildcard {parquet_paths!r}"
    elif isinstance(parquet_paths, list):
        summary = f"using {len(parquet_paths)} parquet paths"

    view_name = register_dataset_relation(client, dataset, rel)
    log.info(f"registered view: {view_name!r} {summary}")
    return view_name


def register_dataset_relation(
    client: duckdb.DuckDBPyConnection,
    dataset: str,
    rel: duckdb.DuckDBPyRelation,
) -> str:
    """Single entrypoing for view registration.

    Going through this function to registere all views lets us centralize the
    table sanitization function so we don't have to worry that dataset names
    are sanitized correctly on various calling points.
    """
    view_name = sanitized_table_name(dataset)

    client.register(
        view_name=view_name,
        python_object=rel,
    )
    return view_name
