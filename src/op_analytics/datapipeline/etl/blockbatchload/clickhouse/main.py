from datetime import date

from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr

from .insert import InsertTask
from .markers import MARKER_COLUMNS, candidate_markers, existing_markers
from .table import BlockBatchTable

log = structlog.get_logger()


def load_to_clickhouse(range_spec: str | None = None):
    """Load blockbatch datasets to Clickhouse for the given date range."""

    # Default to operating over the last 3 days.
    date_range = DateRange.from_spec(range_spec or "m4days")

    candidate_markers_df = candidate_markers(date_range)
    existing_markers_df = existing_markers(date_range)

    # Find markers that exist in candidate_markers_df but not in existing_markers_df
    markers_df = candidate_markers_df.join(existing_markers_df, on=MARKER_COLUMNS, how="anti")
    log.info(f"{len(markers_df)}/{len(candidate_markers_df)} markers pending load to Clickhouse.")

    for (root_path,), group_df in markers_df.group_by("root_path"):
        assert isinstance(root_path, str)
        table = BlockBatchTable(root_path)

        if table.exists():
            continue

        # Attempt to create the table.
        created = table.create()

        # The table does not exist and could not be created.
        # Use the first data path to infer the schema and propose DDL for the table.
        if not created:
            table.raise_not_exists(data_path=group_df["data_path"].to_list()[0])

    tasks: list[InsertTask] = []
    for row in markers_df.to_dicts():
        assert isinstance(row["dt"], date)

        insert_task = InsertTask(
            root_path=row["root_path"],
            chain=row["chain"],
            dt=row["dt"],
            min_block=row["min_block"],
            data_path=row["data_path"],
        )
        tasks.append(insert_task)

    log.info(f"Prepared {len(tasks)} insert tasks.")

    summary = []
    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii+1}/{num_tasks}"):
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

    return summary
