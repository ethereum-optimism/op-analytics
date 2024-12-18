from op_analytics.coreutils.clickhouse.client import init_client as init_clickhouse
from op_analytics.coreutils.duckdb_inmem.client import init_client as init_duckdb
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()


def create_oplabs_views():
    """Create views on external GCS data on the OPLabs Clickhouse database."""

    client = init_clickhouse("OPLABS")
    key = env_get("GCS_HMAC_ACCESS_KEY")
    value = env_get("GCS_HMAC_SECRET")

    # Create the tables we need.
    for database, table in [
        ("intermediate_models", "create_traces_v1"),
    ]:
        ddl_path = repo_path(f"ddl/clickhouse_oplabs/{database}.{table}.sql")
        assert ddl_path is not None
        with open(ddl_path, "r") as fobj:
            statements = (
                fobj.read().replace("HMAC_KEY", key).replace("HMAC_VALUE", value).split(";")
            )
            for i, statement in enumerate(statements):
                log.info(
                    f"running statement for table {table!r}", statement=f"{i+1}/{len(statements)}"
                )
                client.query(statement)
        log.info(f"created '{database}.{table}'")


def view_parquet_schema(path: str):
    """Utility to print out the schema of a parquet file path.

    - Can be a remote path.
    - This utility is useful to generate schemas for clickhouse table DDL.
    """
    ctx = init_duckdb()
    rel = ctx.client.read_parquet(path)
    ctx.client.register(
        view_name="create_traces",
        python_object=rel,
    )
    for col, dtype in rel.pl().schema.items():
        print(f"`{col}` {dtype},")
