from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr

from .markers import MARKER_COLUMNS, existing_markers
from .transform import TransformTask

log = structlog.get_logger()


def execute_dt_transforms(
    group_name: str,
    range_spec: str | None = None,
    start_at_index: int = 0,
    skip_create: bool = True,
    force_complete: bool = False,
    max_tasks: int | None = None,
):
    """Execute "dt" transformations from a specified "group_name" directory."""

    # Default to operating over the last 3 days.
    date_range = DateRange.from_spec(range_spec or "m2days")

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
    markers_df = candidate_markers_df.join(existing_markers_df, on=MARKER_COLUMNS, how="anti")
    log.info(f"{len(markers_df)}/{len(candidate_markers_df)} markers pending.")

    # Prepare transforms.
    tasks: list[TransformTask] = []
    for row in markers_df.to_dicts():
        assert isinstance(row["dt"], date)
        transform_task = TransformTask(
            group_name=row["transform"],
            dt=row["dt"],
            skip_create=skip_create,
            start_at_index=start_at_index,
        )
        tasks.append(transform_task)
    log.info(f"Prepared {len(tasks)} transform tasks.")

    # Execute transforms.
    summary = {}
    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii+1}/{num_tasks}"):
            result = task.execute(start_at_index=start_at_index)

        summary[date_tostr(task.dt)] = result

        if max_tasks is not None and ii + 1 >= max_tasks:
            break
    return summary
