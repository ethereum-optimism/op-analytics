from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs

from .schemas import OVERWRITE_SCHEMAS, INCREMENTAL_SCHEMAS


log = structlog.get_logger()


def execute_pull():
    results = {}
    for table in OVERWRITE_SCHEMAS + INCREMENTAL_SCHEMAS:
        with bound_contextvars(table_name=table.table_name()):
            # Create the table in the OP Labs ClickHouse instance.
            log.info("creating table")
            run_statememt_oplabs(table.create())

            log.info("begin ingestion")
            # Set up the filter clause (this is empty for overwrite tables) and the
            # SELECT statement to read from the Agora public bucket.
            filter_clause = table.incremental_filter_clause()
            select_stmt = table.select(filter_clause)

            # Set up the INSERT statement to write to the OP Labs ClickHouse instance.
            insert_stmt = f"""
            INSERT INTO {table.table_name()}
            {select_stmt}
            """
            if filter_clause:
                log.info(filter_clause)

            # Run the INSERT statement.
            result = run_statememt_oplabs(
                insert_stmt,
                settings={"input_format_csv_use_best_effort_in_schema_inference": 0},
            )
            log.info(f"INSERT INTO {table.table_name()}", **result)

        results[table.table_name()] = result
    return results
