from dataclasses import dataclass


from op_analytics.coreutils.logger import structlog, bound_contextvars

from .location import DataLocation
from .output import OutputData, ExpectedOutput
from .status import all_outputs_complete
from .writehelper import ParqueWriteManager

log = structlog.get_logger()


@dataclass
class DataWriter:
    """Manages writing data and markers consistently."""

    # Sinks
    write_to: list[DataLocation]

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
            sinks=self.write_to,
            markers=[_.marker_path for _ in self.expected_outputs.values()],
            markers_table=self.markers_table,
        )

    def write_all(self, outputs: list[OutputData]):
        """Write data and markers to all the specified locations.

        The data is provided as a list of functions that return a dataframe. This lets us generalize
        the way in which different tasks produce OutputDataFrame.
        """
        for location in self.write_to:
            for output_data in outputs:
                self.write(location, output_data)

    def write(self, location: DataLocation, output_data: OutputData):
        expected_output = self.expected_outputs[output_data.dataset_name]

        # The default partition value is included in logs because it includes
        # the dt value, which helps keep track of where we are when we run a
        # backfill.
        with bound_contextvars(**(output_data.default_partition or {})):
            manager = ParqueWriteManager(
                location=location,
                expected_output=expected_output,
                markers_table=self.markers_table,
                force=self.force,
            )

            manager.write(output_data)
