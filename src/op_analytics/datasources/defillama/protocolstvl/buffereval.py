from datetime import date, timedelta

import polars as pl

from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import TablePath
from op_analytics.coreutils.time import date_fromstr

from ..dataaccess import DefiLlama

log = structlog.get_logger()


def get_buffered(process_dt: date):
    """Slugs that have been processed before.

    Find which slugs have already been written to the ingestion buffer in ClickHouse.

    This includes a data quality check. We consider a slug complete at "process_dt"
    if it has data at the 3 most recent "dt" values with respect to (and including)
    "process_dt".
    """

    table1: TablePath = DefiLlama.PROTOCOLS_TVL.clickhouse_buffer_table()
    table2: TablePath = DefiLlama.PROTOCOLS_TOKEN_TVL.clickhouse_buffer_table()

    slugs = []
    for table in [table1, table2]:
        df = run_query_oplabs(
            query=f"""
                SELECT
                    protocol_slug,
                    COUNT(DISTINCT dt) AS num_dts
                FROM {table.db}.{table.table} FINAL
                WHERE 
                    process_dt = {{param1:Date}}
                    AND dt BETWEEN dateAdd(day, -2, {{param1:Date}}) 
                        AND {{param1:Date}}
                GROUP BY protocol_slug
                """,
            parameters={
                "param1": process_dt,
            },
        )

        # We are looking at 3 dt values, -2, -1, 0.. Only slugs that
        # have data for all 3 are considered complete.
        complete_slugs = df.filter(pl.col("num_dts") == 3)["protocol_slug"].to_list()
        slugs.append(set(complete_slugs))

    return slugs[0].intersection(slugs[1])


def evaluate_buffer(process_dt: date) -> date:
    """Evaluate the buffer and return the last date with complete data.

    Find dt values that are incomplete at process_dt. Alter table to delete data
    from the imcomplete dt values. This prevents us from writing bad data to GCS.
    """
    table1: TablePath = DefiLlama.PROTOCOLS_TVL.clickhouse_buffer_table()

    # Query ClickHouse to the total rows per dt and number of distinct protocol
    # slugs for the (-15 days, -10 days) period and for each of the different dt's
    # on the last 5 days.

    # Evaluate each of the dt values on the last 5 days against the total rows and
    # distintc protocol count averages for the (-15 days, -10 days) period. For the
    # dt to be valid we need the observed values to be within 5% of the averages.

    baseline_query = f"""
        -- Get baseline metrics from -15 to -10 days
        WITH
        baseline AS (
            SELECT
                avg(row_count) as avg_rows,
                avg(protocol_count) as avg_protocols
            FROM (
                SELECT
                    dt,
                    count(*) as row_count,
                    count(distinct protocol_slug) as protocol_count
                FROM {table1.db}.{table1.table} FINAL
                WHERE
                    process_dt = {{process_dt:Date}}
                    AND dt BETWEEN dateAdd(day, -15, {{process_dt:Date}}) 
                        AND dateAdd(day, -6, {{process_dt:Date}})
                GROUP BY dt
            )
        ),

        -- Get metrics for recent days
        recent_metrics AS (
            SELECT
                dt,
                count(*) as row_count,
                count(distinct protocol_slug) as protocol_count
            FROM {table1.db}.{table1.table} FINAL
            WHERE
                process_dt = {{process_dt:Date}}
                AND dt BETWEEN dateAdd(day, -5, {{process_dt:Date}})
                    AND {{process_dt:Date}}
            GROUP BY dt
        )

        -- Compare recent metrics against baseline
        SELECT
            formatDateTime(rm.dt, \'%Y-%m-%d\') as dtstr,
            rm.dt,
            b.avg_rows,
            b.avg_protocols,
            rm.row_count,
            rm.protocol_count,
            abs(rm.row_count - b.avg_rows) / b.avg_rows > 0.05 as rows_outside_threshold,
            abs(rm.protocol_count - b.avg_protocols) / b.avg_protocols > 0.05 as protocols_outside_threshold
        FROM recent_metrics rm
        CROSS JOIN baseline b
        ORDER BY dtstr
    """

    results_df = run_query_oplabs(
        baseline_query,
        parameters={"process_dt": process_dt},
    )
    print(results_df)
    results = results_df.to_dicts()

    # Identify dates with incomplete data
    bad_dts: list[str] = [
        row["dtstr"]
        for row in results
        if row["rows_outside_threshold"] or row["protocols_outside_threshold"]
    ]

    last_complete_date: date

    if bad_dts:
        # Find the first bad date. We'll need to remove data from that one and
        # all subsequente dates up until process_dt.
        first_bad_date = date_fromstr(min(bad_dts))
        last_complete_date = first_bad_date - timedelta(days=1)

        log.warning(f"Detected incomplete data for {bad_dts}")
    else:
        log.info("Did not detect incomplete data in the buffer.")
        last_complete_date = process_dt

    return last_complete_date
