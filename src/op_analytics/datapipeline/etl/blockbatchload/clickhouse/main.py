from datetime import date
from dataclasses import dataclass
from op_analytics.coreutils.clickhouse.ddl import ClickHouseTable
from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr

from .insert import InsertTask
from .markers import MARKER_COLUMNS, candidate_markers, existing_markers
from .table import BlockBatchTable

log = structlog.get_logger()


@dataclass
class GCSData:
    root_path: str
    enforce_row_count: bool = True


def load_to_clickhouse(
    datasets: list[GCSData],
    range_spec: str | None = None,
    max_tasks: int | None = None,
):
    """Load blockbatch datasets to Clickhouse for the given date range."""

    # Default to operating over the last 3 days.
    date_range = DateRange.from_spec(range_spec or "m4days")

    # Indexed by root_path
    datasets_by_root_path = {d.root_path: d for d in datasets}

    # Candidate markers are for blockbatches produced upstream.
    candidate_markers_df = candidate_markers(
        date_range=date_range,
        root_paths=[d.root_path for d in datasets],
    )

    # Existing markers are for blockbatches already loaded to ClickHouse.
    existing_markers_df = existing_markers(date_range)

    # Find markers that exist in candidate_markers_df but not in existing_markers_df
    markers_df = candidate_markers_df.join(existing_markers_df, on=MARKER_COLUMNS, how="anti")
    log.info(f"{len(markers_df)}/{len(candidate_markers_df)} markers pending load to Clickhouse.")

    for (root_path,), group_df in markers_df.group_by("root_path"):
        assert isinstance(root_path, str)

        table: ClickHouseTable = BlockBatchTable(root_path).to_table()

        # When the table does not exist use the first data path to infer the schema
        # and propose DDL for the table.
        table.create_if_not_exists(data_path=group_df["data_path"].to_list()[0])

    tasks: list[InsertTask] = []
    for row in markers_df.to_dicts():
        assert isinstance(row["dt"], date)
        assert isinstance(row["root_path"], str)

        bbtable = BlockBatchTable(row["root_path"]).to_table()

        insert_task = InsertTask(
            root_path=row["root_path"],
            table_name=bbtable.table_name,
            chain=row["chain"],
            dt=row["dt"],
            min_block=row["min_block"],
            data_path=row["data_path"],
            enforce_row_count=datasets_by_root_path[row["root_path"]].enforce_row_count,
        )
        tasks.append(insert_task)

    log.info(f"Prepared {len(tasks)} insert tasks.")

    summary = []
    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii + 1}/{num_tasks}"):
            result = task.execute()

        summary.append(
            dict(
                dt=date_tostr(task.dt),
                chain=task.chain,
                table=task.table_name,
                min_block=task.min_block,
                data_path=task.data_path,
                written_rows=result.written_rows,
            )
        )

        if max_tasks is not None and ii + 1 >= max_tasks:
            break

    return summary
