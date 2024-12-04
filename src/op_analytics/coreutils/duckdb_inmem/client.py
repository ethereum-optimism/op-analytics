from threading import Lock

import duckdb

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


_DUCK_DB: duckdb.DuckDBPyConnection | None = None

_INIT_LOCK = Lock()


def init_client():
    global _DUCK_DB

    with _INIT_LOCK:
        if _DUCK_DB is None:
            _DUCK_DB = duckdb.connect()

            # Setup access to GCS
            _DUCK_DB.sql("INSTALL httpfs")
            KEY_ID = env_get("GCS_HMAC_ACCESS_KEY")
            SECRET = env_get("GCS_HMAC_SECRET")
            _DUCK_DB.sql(f"""
            CREATE SECRET (
                TYPE GCS,
                KEY_ID '{KEY_ID}',
                SECRET '{SECRET}'
            );
            """)

    if _DUCK_DB is None:
        raise RuntimeError("DuckDB client was not properly initialized.")

    return _DUCK_DB


def register_parquet_relation(dataset: str, parquet_paths: list[str] | str) -> str:
    """Return a DuckDB relation from a list of parquet files."""
    client = init_client()

    rel = client.read_parquet(parquet_paths, hive_partitioning=True)

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


def sanitized_table_name(dataset_name: str) -> str:
    return dataset_name.replace("/", "_")
