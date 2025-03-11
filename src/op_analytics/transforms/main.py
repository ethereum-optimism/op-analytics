import os
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr, now_date

from .create import TableStructure, create_tables
from .markers import MARKER_COLUMNS, existing_markers
from .task import NoWrittenRows, TransformTask

log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


def execute_dt_transforms(
    group_name: str,
    range_spec: str | None = None,
    update_only: list[str] | None = None,
    raise_if_empty: bool = True,
    force_complete: bool = False,
):
    """Execute "dt" transformations from a specified "group_name" directory."""

    group_dir = os.path.join(DIRECTORY, group_name)
    if not os.path.isdir(group_dir):
        raise Exception(f"group={group_name} is not valid: directory does not exist: {group_dir}")

    # Default to operating over the last 3 days.
    date_range = DateRange.from_spec(range_spec or "m2days")

    # Create tables for this group.
    tables: dict[str, TableStructure] = create_tables(group_name=group_name)

    # Candidates are all markers in the range_spec for the requested group
    candidate_markers_df = pl.DataFrame(
        [{"dt": dt, "transform": group_name} for dt in date_range.dates()]
    )

    # Get existing markers.
    existing_markers_df = existing_markers(
        transforms=[group_name],
        date_range=date_range,
    )

    if force_complete:
        existing_markers_df = existing_markers_df.filter(False)

    # Find which of the transform/dt pairs in the range_spec have not been processed yet.
    pending_markers = candidate_markers_df.join(
        existing_markers_df,
        on=MARKER_COLUMNS,
        how="anti",
    ).to_dicts()
    log.info(f"{len(pending_markers)}/{len(candidate_markers_df)} markers pending.")

    # Prepare transforms.
    tasks: list[TransformTask] = []
    for row in pending_markers:
        assert isinstance(row["dt"], date)
        transform_task = TransformTask(
            group_name=row["transform"],
            dt=row["dt"],
            tables=tables,
            update_only=update_only,
            raise_if_empty=raise_if_empty,
        )
        tasks.append(transform_task)
    log.info(f"Prepared {len(tasks)} transform tasks.")

    # Execute updates for this group.
    summary = {}
    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii+1}/{num_tasks}", dt=date_tostr(task.dt)):
            try:
                result = task.execute()
            except NoWrittenRows:
                if task.dt == now_date():
                    # It is possible that NoWrittenRows is encountered when we are running
                    # close to real time. In that case don't error out the run, log a warning
                    # and exit gracefully.
                    msg = (
                        "no written rows detected on the current date. will stop further processing"
                    )
                    log.warning(msg)
                    summary[date_tostr(task.dt)] = dict(error=msg)
                    break
                else:
                    raise

            else:
                summary[date_tostr(task.dt)] = result

    return summary
