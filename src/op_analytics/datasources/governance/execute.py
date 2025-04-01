from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs
from op_analytics.coreutils.logger import bound_contextvars, structlog

from .dataaccess import Governance

log = structlog.get_logger()


def execute_pull():
    results = {}
    for table in Governance.all_tables():
        fqname = f"{table.db}.{table.table}"

        with bound_contextvars(table_name=fqname):
            # Create table if not exists.
            table.create_table()

            # Run the INSERT statement.
            log.info("begin ingestion")
            insert_statement = table.read_sql(subdir="ingest")

            result = run_statememt_oplabs(
                insert_statement,
                settings={"input_format_csv_use_best_effort_in_schema_inference": 0},
            )
            log.info(f"INSERT INTO {fqname}", **result)

            results[fqname] = result

    return results
