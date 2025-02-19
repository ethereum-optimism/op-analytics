from dataclasses import asdict, dataclass
from datetime import date

import polars as pl
import pyarrow as pa

from op_analytics.coreutils.clickhouse.gcswrite import write_to_gcs
from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import TablePath
from op_analytics.coreutils.partitioned.marker import Marker
from op_analytics.coreutils.partitioned.partition import (
    Partition,
    PartitionColumn,
    PartitionMetadata,
)
from op_analytics.coreutils.time import date_tostr

from .dailydata import DailyDataset
from .dailydatawrite import (
    EXTRA_MARKER_COLUMNS,
    EXTRA_MARKER_COLUMNS_SCHEMA,
    MARKERS_TABLE,
    PARQUET_FILENAME,
    expected_output_of,
)
from .dataaccess import init_data_access
from .location import DataLocation

log = structlog.get_logger()


@dataclass
class WriteSummary:
    """Summary for what was written to the root_path."""

    root_path: str

    # Total number of rows written across all "dt"s.
    written_rows: int

    # Number of "dt"s written.
    written_dts: int

    def to_dict(self):
        return asdict(self)


@dataclass
class SinglePartitionSummary:
    """Summary for what was written to a single "dt" partition."""

    gcs_path: str
    written_rows: int


@dataclass
class FromClickHouseWriter:
    """Write ClickHouse data to GCS DailyData.

    Usually we ingest data in memory as polars dataframes and then write it out
    directly to GCS. For some of the more intensive data pulls we use ClickHouse
    as a buffer, streaming writes into a buffer table and then dumping the buffer
    table to GCS.

    This class is a utility to take a ClickHouse buffer table with "process_dt" and
    "dt" columns and then write a range of "dt" partitions into GCS for a given
    "process_dt".

    The write is performed in a way that is compatible with the DailyData pattern.
    "/dt=YYYY-MM-DD" partitions are created in GCS and markers are written for each
    "dt" partition on the ClickHouse DailyData markers table.
    """

    dailydata_table: DailyDataset
    process_dt: date
    min_dt: date
    max_dt: date
    order_by: str

    @property
    def buffer_table(self) -> TablePath:
        """Path to the ClickHouse table that holds the buffered data."""

        return self.dailydata_table.clickhouse_buffer_table()

    def write(self) -> WriteSummary:
        """Write out the data and markers."""

        # Find out what will be written.
        markers_df = self.get_markers_df()
        total_rows = markers_df["row_count"].sum()
        log.info(f"found data for {len(markers_df)} dt's with {total_rows} total rows")

        # Write out the data.
        for ii, marker in enumerate(markers_df.to_dicts()):
            dtstr = date_tostr(marker["dt"])
            result: SinglePartitionSummary = self.write_single_dt(dt=dtstr)
            log.info(
                f"{ii+1:03d}/{len(markers_df):03d} wrote {result.written_rows} rows to gcs at {result.gcs_path}"
            )

            # The result of what was written should agree with the markers.
            if marker["row_count"] != result.written_rows:
                raise Exception(
                    f"error encountered when writing data to GCS from ClickHouse: row counts dont match: {self.dailydata_table.root_path}"
                )

        log.info(f"wrote {total_rows} total rows to gcs at {self.dailydata_table.table}")

        # Write out the markers.
        self.write_markers(markers_df)

        return WriteSummary(
            root_path=self.dailydata_table.root_path,
            written_rows=total_rows,
            written_dts=len(markers_df),
        )

    def from_filtered(self):
        """SQL to select and filter data from ClickHouse."""

        return f"""
            FROM {self.buffer_table.db}.{self.buffer_table.table} FINAL
            WHERE process_dt = '{date_tostr(self.process_dt)}'
            AND dt >= '{date_tostr(self.min_dt)}'
            AND dt <= '{date_tostr(self.max_dt)}'
            """

    def dt_counts(self):
        """SQL to count number of rows by dt."""

        return f"""
            SELECT toString(dt) AS dt, count(*) as row_count
            {self.from_filtered()}
            GROUP BY 1 ORDER BY 1
            """

    def get_dt_counts(self) -> list[dict]:
        return run_query_oplabs(query=self.dt_counts()).sort("dt").to_dicts()

    def get_markers_df(self):
        """Generate the markers dataframe.

        We write one merker for each "dt" partition in the data.
        """

        marker_dfs = []
        for dt_info in self.get_dt_counts():
            datestr = dt_info["dt"]
            partition = Partition([PartitionColumn(name="dt", value=datestr)])
            partition_meta = PartitionMetadata(row_count=dt_info["row_count"])

            marker = Marker(
                expected_output=expected_output_of(
                    root_path=self.dailydata_table.root_path,
                    datestr=datestr,
                ),
                written_parts={partition: partition_meta},
            )

            marker_df = marker.to_pyarrow_table(
                process_name="from_clickhouse",
                extra_marker_columns=EXTRA_MARKER_COLUMNS,
                extra_marker_columns_schema=EXTRA_MARKER_COLUMNS_SCHEMA,
            )
            marker_dfs.append(marker_df)

        # Concatenate all marker dataframes
        return pl.from_arrow(pa.concat_tables(marker_dfs))

    def write_markers(self, marker_df: pl.DataFrame):
        """Insert the markers to ClickHouse."""

        client = init_data_access()
        client.write_marker(
            marker_df=marker_df.to_arrow(),
            data_location=DataLocation.GCS,
            markers_table=MARKERS_TABLE,
        )

    def write_single_dt(self, dt: str) -> SinglePartitionSummary:
        """Write data for the given "dt" to GCS."""

        select = f"""
            SELECT * EXCEPT(dt, process_dt)
            {self.from_filtered()}
            AND dt = '{dt}'
            ORDER BY {self.order_by}
            """

        gcs_path = (
            f"oplabs-tools-data-sink/{self.dailydata_table.root_path}/dt={dt}/{PARQUET_FILENAME}"
        )

        result = write_to_gcs(
            gcs_path=gcs_path,
            select=select,
        )
        return SinglePartitionSummary(
            gcs_path=gcs_path,
            written_rows=result.written_rows,
        )
