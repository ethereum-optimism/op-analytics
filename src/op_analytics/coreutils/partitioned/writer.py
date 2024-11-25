from collections import defaultdict
from dataclasses import dataclass

from op_analytics.coreutils.logger import bound_contextvars, structlog, human_rows

from .location import DataLocation
from .output import ExpectedOutput, OutputData, OutputPartMeta
from .status import all_outputs_complete
from .writehelper import ParquetWriteManager

log = structlog.get_logger()


@dataclass
class DataWriter:
    """Manages writing data and markers consistently."""

    # Data store where we write the data.
    write_to: DataLocation

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

    def write_all(self, outputs: list[OutputData]):
        """Write data and markers to all the specified locations.

        The data is provided as a list of functions that return a dataframe. This lets us generalize
        the way in which different tasks produce OutputDataFrame.
        """

        total_rows: dict[str, int] = defaultdict(int)

        for output_data in outputs:
            parts = self.write(self.write_to, output_data)

            for part in parts:
                total_rows[output_data.dataset_name] += part.row_count

        summary = " ".join(f"{key}={human_rows(val)}" for key, val in total_rows.items())
        summary = f"{self.write_to.name}::{summary}"
        log.info(f"done writing. {summary}")

    def write(self, location: DataLocation, output_data: OutputData) -> list[OutputPartMeta]:
        expected_output = self.expected_outputs[output_data.dataset_name]

        # The default partition value is included in logs because it includes
        # the dt value, which helps keep track of where we are when we run a
        # backfill.

        with bound_contextvars(
            dataset=output_data.dataset_name, **(output_data.default_partition or {})
        ):
            manager = ParquetWriteManager(
                location=location,
                expected_output=expected_output,
                markers_table=self.markers_table,
                force=self.force,
            )

            return manager.write(output_data)
