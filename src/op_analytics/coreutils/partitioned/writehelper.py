from dataclasses import dataclass
from typing import Any

from overrides import EnforceOverrides

from op_analytics.coreutils.logger import structlog

from .dataaccess import init_data_access
from .location import DataLocation
from .output import ExpectedOutput, OutputPartMeta

log = structlog.get_logger()


@dataclass
class WriteManager(EnforceOverrides):
    """Helper class that allows arbitrary write logic and handles completion markers.

    - If completion markers exist no data is written.
    - If data is written correctly commpletion markers are created.
    """

    location: DataLocation
    expected_output: ExpectedOutput
    markers_table: str
    force: bool

    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        raise NotImplementedError()

    def write(self, output_data: Any) -> list[OutputPartMeta]:
        client = init_data_access()

        is_complete = client.marker_exists(
            data_location=self.location,
            marker_path=self.expected_output.marker_path,
            markers_table=self.markers_table,
        )

        if is_complete and not self.force:
            log.info(
                f"[{self.location.name}] Skipping already complete output at {self.expected_output.marker_path}"
            )
            return []

        written_parts = self.write_implementation(output_data)

        client.write_marker(
            data_location=self.location,
            expected_output=self.expected_output,
            written_parts=written_parts,
            markers_table=self.markers_table,
        )
        log.debug(f"done writing {self.expected_output.dataset_name} to {self.location.name}")

        return written_parts
