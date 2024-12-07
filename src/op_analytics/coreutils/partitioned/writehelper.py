from dataclasses import dataclass, field
from typing import Protocol

from overrides import EnforceOverrides

from op_analytics.coreutils.logger import structlog, bound_contextvars

from .dataaccess import init_data_access, all_outputs_complete
from .location import DataLocation
from .marker import Marker
from .output import ExpectedOutput
from .partition import WrittenParts

log = structlog.get_logger()


@dataclass
class WriteResult:
    status: str
    written_parts: WrittenParts


class Writeable(Protocol):
    @property
    def root_path(self) -> str: ...

    @property
    def default_partition(self) -> dict[str, str] | None: ...


@dataclass
class WriteManager[T: Writeable](EnforceOverrides):
    """Helper class that allows arbitrary write logic and handles completion markers.

    - If completion markers exist no data is written.
    - If data is written correctly commpletion markers are created.
    """

    # Location where data will be written.
    location: DataLocation

    # Table where markers will be inserted.
    markers_table: str

    # Expected Outputs
    expected_outputs: list[ExpectedOutput]

    # If true, writes data even if markers already exist.
    force: bool

    # Internal state for status of completion markers.
    _is_complete: bool | None = field(default=None, init=False, repr=False)

    # Expected outputs by name (post-init).
    _keyed_outputs: dict[str, ExpectedOutput] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self):
        for output in self.expected_outputs:
            self._keyed_outputs[output.root_path] = output

        if len(self.expected_outputs) != len(self._keyed_outputs):
            raise ValueError("expected output names are not unique")

    def is_complete(self) -> bool:
        if self._is_complete is None:
            self._is_complete = all_outputs_complete(
                location=self.location,
                markers=[_.marker_path for _ in self.expected_outputs],
                markers_table=self.markers_table,
            )
        return self._is_complete

    def expected_output(self, output_data: T) -> ExpectedOutput:
        return self._keyed_outputs[output_data.root_path]

    def write_implementation(self, output_data: T) -> WrittenParts:
        raise NotImplementedError()

    def write(self, output_data: T) -> WriteResult:
        self.location.check_write_allowed()

        # Locate the expected output that coresponds to the given output_data.
        expected_output = self.expected_output(output_data)

        # The default partition value is included in log context to help keep
        # track of which data we are processing.
        with bound_contextvars(root=output_data.root_path, **(output_data.default_partition or {})):
            client = init_data_access()

            is_complete = client.marker_exists(
                data_location=self.location,
                marker_path=expected_output.marker_path,
                markers_table=self.markers_table,
            )

            if is_complete and not self.force:
                log.info(
                    f"[{self.location.name}] Skipping already complete output at {expected_output.marker_path}"
                )
                return WriteResult(status="skipped", written_parts={})

            written_parts = self.write_implementation(output_data)

            marker = Marker(
                expected_output=expected_output,
                written_parts=written_parts,
            )

            client.write_marker(
                data_location=self.location,
                marker=marker,
                markers_table=self.markers_table,
            )
            log.debug(f"done writing {expected_output.root_path} to {self.location.name}")

            return WriteResult(status="success", written_parts=written_parts)
