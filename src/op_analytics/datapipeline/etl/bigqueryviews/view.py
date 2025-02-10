import os
from typing import Literal

from op_analytics.coreutils.bigquery.write import init_client
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


def load_select(db_name: str, view_name: str):
    ddl_path = os.path.join(DIRECTORY, f"ddl/{db_name}/{view_name}.sql")
    if not os.path.exists(ddl_path):
        raise Exception(f"Cound not find ddl file for {db_name}{view_name} at {ddl_path}")

    with open(ddl_path, "r") as f:
        return f.read()


def create_view(
    db_name: str,
    view_name: str,
    disposition: Literal["replace"] | Literal["if_not_exists"] = "if_not_exists",
):
    """Create a BigQuery VIEW."""

    client = init_client()

    # Execute the DDL statement
    select = load_select(db_name, view_name)

    approach: str
    if disposition == "if_not_exists":
        approach = "VIEW IF NOT EXISTS"
    elif disposition == "replace":
        approach = "OR REPLACE VIEW"
    else:
        raise NotImplementedError(f"invalid disposition: {disposition}")

    ddl_statement = f"""
    CREATE {approach} `oplabs-tools-data.{db_name}.{view_name}` AS 
    {select}
    """

    query_job = client.query(ddl_statement)
    query_job.result()  # Wait for the job to complete
    log.info(f"created bigquery view: {db_name}.{view_name}")
