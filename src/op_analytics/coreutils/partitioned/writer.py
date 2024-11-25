from dataclasses import dataclass
from typing import Any

from overrides import override

from op_analytics.coreutils.logger import bound_contextvars, structlog

from .breakout import breakout_partitions
from .dataaccess import init_data_access
from .location import DataLocation
from .output import ExpectedOutput, OutputData, OutputPartMeta
from .status import all_outputs_complete
from .writehelper import WriteManager

log = structlog.get_logger()


@dataclass
class DataWriter:
    """Manages writing data and markers consistently."""

    # Data store where we write the data.
    write_to: DataLocation

    # Partition columns
    partition_cols: list[str]

    # Markers Table
    markers_table: str

    # Expected Outputs
    expected_outputs: dict[str, ExpectedOutput]

    # Is set to true if all markers already exist.
    is_complete: bool

    # If true, writes data even if markers already exist.
    force: bool

    def all_complete(self) -> bool:
        """Check if all expected markers are complete."""
        return all_outputs_complete(
            location=self.write_to,
            markers=[_.marker_path for _ in self.expected_outputs.values()],
            markers_table=self.markers_table,
        )

    def write(self, output_data: OutputData) -> list[OutputPartMeta]:
        """Write data and corresponding marker."""
        expected_output = self.expected_outputs[output_data.dataset_name]

        # The default partition value is included in logs because it includes
        # the dt value, which helps keep track of where we are when we run a
        # backfill.

        with bound_contextvars(
            dataset=output_data.dataset_name, **(output_data.default_partition or {})
        ):
            manager = PartitionedWriteManager(
                partition_cols=self.partition_cols,
                location=self.write_to,
                expected_output=expected_output,
                markers_table=self.markers_table,
                force=self.force,
            )

            return manager.write(output_data)


@dataclass
class PartitionedWriteManager(WriteManager):
    partition_cols: list[str]

    @override
    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        assert isinstance(output_data, OutputData)

        client = init_data_access()

        parts = breakout_partitions(
            df=output_data.dataframe,
            partition_cols=self.partition_cols,
            default_partition=output_data.default_partition,
        )

        parts_meta: list[OutputPartMeta] = []
        for part in parts:
            client.write_single_part(
                location=self.location,
                dataframe=part.df,
                full_path=part.meta.full_path(
                    self.expected_output.root_path, self.expected_output.file_name
                ),
            )
            parts_meta.append(part.meta)

        return parts_meta
