from dataclasses import dataclass

from overrides import EnforceOverrides

from op_analytics.coreutils.logger import structlog

from .dataaccess import init_data_access
from .location import DataLocation
from .marker import Marker
from .output import ExpectedOutput
from .partition import WrittenParts

log = structlog.get_logger()


@dataclass
class WriteResult:
    status: str
    written_parts: WrittenParts


@dataclass
class WriteManager[T](EnforceOverrides):
    """Helper class that allows arbitrary write logic and handles completion markers.

    - If completion markers exist no data is written.
    - If data is written correctly commpletion markers are created.
    """

    location: DataLocation
    expected_output: ExpectedOutput
    markers_table: str
    force: bool

    def write_implementation(self, output_data: T) -> WrittenParts:
        raise NotImplementedError()

    def write(self, output_data: T) -> WriteResult:
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
            return WriteResult(status="skipped", written_parts={})

        written_parts = self.write_implementation(output_data)

        marker = Marker(
            expected_output=self.expected_output,
            written_parts=written_parts,
        )

        client.write_marker(
            data_location=self.location,
            marker=marker,
            markers_table=self.markers_table,
        )
        log.debug(f"done writing {self.expected_output.root_path} to {self.location.name}")

        return WriteResult(status="success", written_parts=written_parts)
