from threading import Lock


import duckdb
from op_analytics.coreutils.env.vault import env_get


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


def parquet_relation(parquet_paths) -> duckdb.DuckDBPyRelation:
    """Return a DuckDB relation from a list of parquet files."""
    client = init_client()

    paths_str = ", ".join(f"'{_}'" for _ in parquet_paths)

    # TODO: Investigate if using read_parquet is more performant.
    return client.sql(
        f"SELECT * FROM read_parquet([{paths_str}], hive_partitioning = true, hive_types_autocast = 0)"
    )
