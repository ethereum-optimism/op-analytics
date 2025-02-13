from contextlib import contextmanager
from functools import cache

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.env.aware import is_bot
from op_analytics.coreutils.logger import structlog

from .breakout import breakout_partitions
from .location import DataLocation
from .output import ExpectedOutput, OutputData
from .writer import PartitionedWriteManager

log = structlog.get_logger()


MARKERS_TABLE = "daily_data_markers"


@cache
def determine_location() -> DataLocation:
    # Only for github actions or k8s we use GCS.
    if is_bot():
        return DataLocation.GCS

    # For unittests and local runs we use LOCAL.
    return DataLocation.LOCAL


@contextmanager
def write_to_prod():
    """Context manager to write data to production from your laptop.

    USE CAREFULLY.

    Example usage:

    >>> with write_to_prod():
    >>>    execute_pull()
    """
    import os
    from unittest.mock import patch

    def mock_location():
        return DataLocation.GCS

    os.environ["ALLOW_WRITE"] = "true"

    with patch(
        "op_analytics.coreutils.partitioned.dailydatawrite.determine_location",
        mock_location,
    ):
        yield


def write_daily_data(
    root_path: str,
    dataframe: pl.DataFrame,
    sort_by: list[str] | None = None,
):
    """Write date partitioned defillama dataset.

    NOTE: This method always overwrites data. If we had already pulled in data for
    a given date a subsequent data pull will always overwrite it.
    """

    parts = breakout_partitions(
        df=dataframe,
        partition_cols=["dt"],
        default_partitions=None,
    )

    # Ensure write location for tests is LOCAL.
    location = determine_location()

    for part in parts:
        datestr = part.partition_value("dt")

        writer = PartitionedWriteManager(
            process_name="default",
            location=location,
            partition_cols=["dt"],
            extra_marker_columns=dict(),
            extra_marker_columns_schema=[
                pa.field("dt", pa.date32()),
            ],
            markers_table=MARKERS_TABLE,
            expected_outputs=[
                ExpectedOutput(
                    root_path=root_path,
                    file_name="out.parquet",
                    marker_path=f"{datestr}/{root_path}",
                )
            ],
        )

        part_df = part.df.with_columns(dt=pl.lit(datestr))

        if sort_by is not None:
            part_df = part_df.sort(*sort_by)

        writer.write(
            output_data=OutputData(
                dataframe=part_df,
                root_path=root_path,
                default_partitions=None,
            )
        )
