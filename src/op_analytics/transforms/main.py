from datetime import date

import polars as pl

from op_analytics.coreutils.clickhouse.ddl import ClickHouseTable
from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.rangeutils.daterange import DateRange

from .config import DIRECTORY, EXECUTABLE_TRANSFORMS
from .markers import MARKER_COLUMNS, existing_markers
from .transform import TransformTask

log = structlog.get_logger()


def execute_dt_transforms(
    range_spec: str | None = None,
    overwrite: bool = False,
    max_tasks: int | None = None,
):
    """Execute "dt" transformations in the Clickhouse data warehouse."""

    # Default to operating over the last 3 days.
    date_range = DateRange.from_spec(range_spec or "m3days")

    # Candidates are all markers in the range_spec for the transforms requested.
    candidate_markers_df = pl.DataFrame(
        [
            {"dt": dt, "transform": transform}
            for transform in EXECUTABLE_TRANSFORMS
            for dt in date_range.dates()
        ]
    )

    # Get existing markers.
    existing_markers_df = existing_markers(
        transforms=EXECUTABLE_TRANSFORMS,
        date_range=date_range,
    )

    if overwrite:
        existing_markers_df = existing_markers_df.filter(False)

    # Find which of the transform/dt pairs in the range_spec have not been processed yet.
    markers_df = candidate_markers_df.join(existing_markers_df, on=MARKER_COLUMNS, how="anti")
    log.info(f"{len(markers_df)}/{len(candidate_markers_df)} markers pending.")

    # Create tables if necessary.
    for transform in markers_df["transform"].unique().to_list():
        assert isinstance(transform, str)

        table = ClickHouseTable(
            ddl_directory=DIRECTORY,
            ddl_path=f"{transform}/CREATE.sql",
            table_db="transforms",
            table_name=transform,
        )
        table.create_if_not_exists()

    # Prepare transforms.
    tasks: list[TransformTask] = []
    for row in markers_df.to_dicts():
        assert isinstance(row["dt"], date)
        transform_task = TransformTask(
            table_name=row["transform"],
            dt=row["dt"],
        )
        tasks.append(transform_task)
    log.info(f"Prepared {len(tasks)} transform tasks.")

    # Execute transforms.
    summary = []
    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii+1}/{num_tasks}"):
            result = task.execute()

        summary.append(result)

        if max_tasks is not None and ii + 1 >= max_tasks:
            break

    return summary
