from dataclasses import dataclass, field
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
    expected_outputs: list[ExpectedOutput]

    # If true, writes data even if markers already exist.
    force: bool

    # Internal state for status of completion markers.
    _is_complete: bool | None = field(default=None, init=False)

    # Expected outputs by name (post-init).
    _keyed_outputs: dict[str, ExpectedOutput] = field(init=False, default_factory=dict)

    def __post_init__(self):
        for output in self.expected_outputs:
            self._keyed_outputs[output.dataset_name] = output

        if len(self.expected_outputs) != len(self._keyed_outputs):
            raise ValueError("expected output names are not unique")

        self.write_to.check_write_allowed()

    def is_complete(self) -> bool:
        if self._is_complete is None:
            self._is_complete = all_outputs_complete(
                location=self.write_to,
                markers=[_.marker_path for _ in self.expected_outputs],
                markers_table=self.markers_table,
            )
        return self._is_complete

    def write(self, output_data: OutputData) -> list[OutputPartMeta]:
        """Write data and corresponding marker."""

        # Locate the expected output that coresponds to the given output_data.
        expected_output = self._keyed_outputs[output_data.dataset_name]

        # The default partition value is included in log context to help keep
        # track of which data we are processing.
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
                full_path=self.expected_output.full_path(part.partitions),
            )
            parts_meta.append(part.meta)

        return parts_meta
