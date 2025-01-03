import os
import tempfile
from dataclasses import dataclass
from threading import Lock

import duckdb

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
            _DUCK_DB.client.sql("INSTALL httpfs")
            KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
            SECRET = env_get("GCS_HMAC_SECRET")
            _DUCK_DB.client.sql(f"""
            CREATE SECRET (
                TYPE GCS,
                KEY_ID '{KEY_ID}',
                SECRET '{SECRET}'
            );
            """)

    if _DUCK_DB is None:
        raise RuntimeError("DuckDB client was not properly initialized.")

    return _DUCK_DB


def sanitized_table_name(dataset_name: str) -> str:
    return dataset_name.replace("/", "_")


class EmptyParquetData(Exception):
    pass


@dataclass
class RemoteParquetData:
    sanitized_name: str
    paths: list[str]

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

        return f"""
        read_parquet(
            [
                {paths_str}
            ],
            hive_partitioning = true
        );
        """

    def create_table(self) -> str:
        ctx = init_client()

        name = f"{self.sanitized_name}_tbl"

        ctx.client.sql(f"""
            CREATE OR REPLACE TABLE {name} AS
            SELECT * FROM {self.read_parquet_string()};
            """)
        log.info(f"created table {name} using {self.num_paths} parquet paths")

        ctx.report_size()
        return name

    def create_view(self) -> str:
        ctx = init_client()

        name = f"{self.sanitized_name}_view"

        ctx.client.sql(f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM {self.read_parquet_string()};
            """)
        log.info(f"created view {name} using {self.num_paths} parquet paths")

        ctx.report_size()
        return name


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
