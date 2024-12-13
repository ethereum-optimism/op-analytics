from dataclasses import dataclass, field
from typing import Any, Protocol

import pyarrow as pa
from overrides import EnforceOverrides

from op_analytics.coreutils.logger import bound_contextvars, structlog

from .dataaccess import init_data_access, complete_markers
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

    # Partition columns for datasets written out by this manager.
    partition_cols: list[str]

    # Values for extra columns stored in the markers table.
    # Some columns are specific to the type of dataset being stored.
    extra_marker_columns: dict[str, Any]

    # Schema for additional columns stored in the markers table.
    # The explicit schema makes it easer to prepare the pyarrow table
    # that is used to write the markers.
    extra_marker_columns_schema: list[pa.Field]

    # Table where markers will be inserted.
    markers_table: str

    # Expected Outputs
    expected_outputs: list[ExpectedOutput]

    # Complete Markers. A list of markers that are already complete.
    # If a marker is already complete then we can skip writing its
    # corresponding output.
    complete_markers: list[str] | None = None

    # Process that is writing data. This can be used to identify backfills for example.
    process_name: str = field(default="default")

    # Internal state for status of completion markers.
    _is_complete: bool | None = field(default=None, init=False, repr=False)

    # Expected outputs by name (post-init).
    _keyed_outputs: dict[str, ExpectedOutput] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self):
        for output in self.expected_outputs:
            self._keyed_outputs[output.root_path] = output

        if len(self.expected_outputs) != len(self._keyed_outputs):
            raise ValueError("expected output names are not unique")

    def all_outputs_complete(self) -> bool:
        expected_markers = [_.marker_path for _ in self.expected_outputs]

        # Use complete markers stored in the task.
        if self.complete_markers is not None:
            return set(self.complete_markers) == set(expected_markers)

        if self._is_complete is None:
            # Query the markers database to find out which markers are complete.
            # TODO: Delete this code. We should always pre-fetch completion markers
            #       so we don't have to make repeated database queries for each task.
            self.complete_markers = complete_markers(
                location=self.location,
                markers=expected_markers,
                markers_table=self.markers_table,
            )

            self._is_complete = set(self.complete_markers) == set(expected_markers)

        return self._is_complete

    def expected_output(self, output_data: T) -> ExpectedOutput:
        return self._keyed_outputs[output_data.root_path]

    def write_implementation(self, output_data: T) -> WrittenParts:
        raise NotImplementedError()

    def write(self, output_data: T) -> WriteResult:
        # Locate the expected output that coresponds to the given output_data.
        expected_output = self.expected_output(output_data)

        # The default partition value is included in log context to help keep
        # track of which data we are processing.
        with bound_contextvars(root=output_data.root_path, **(output_data.default_partition or {})):
            # TODO: This should not be necessary once we fully migrate to pre-fetched markers.
            complete_markers = self.complete_markers or []

            if expected_output.marker_path in complete_markers:
                log.warning(f"skipping complete output {expected_output.marker_path}")
                return WriteResult(status="skipped", written_parts={})

            client = init_data_access()

            self.location.check_write_allowed()
            written_parts = self.write_implementation(output_data)

            marker = Marker(
                expected_output=expected_output,
                written_parts=written_parts,
            )

            marker_df = marker.to_pyarrow_table(
                process_name=self.process_name,
                extra_marker_columns=self.extra_marker_columns,
                extra_marker_columns_schema=self.extra_marker_columns_schema,
            )

            client.write_marker(
                marker_df=marker_df,
                data_location=self.location,
                markers_table=self.markers_table,
            )
            log.debug(f"done writing {expected_output.root_path} to {self.location.name}")

            return WriteResult(status="success", written_parts=written_parts)
