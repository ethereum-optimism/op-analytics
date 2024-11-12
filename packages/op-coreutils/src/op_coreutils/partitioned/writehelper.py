from dataclasses import dataclass
from typing import Any

from overrides import EnforceOverrides, override

from op_coreutils.logger import structlog

from .breakout import breakout_partitions
from .dataaccess import init_data_access
from .location import DataLocation
from .output import OutputData, ExpectedOutput, OutputPartMeta

log = structlog.get_logger()


@dataclass
class WriteManager(EnforceOverrides):
    """Helper class that allows arbitrary write logic and handles completion markers.

    - If complention markers exist no data is written.
    - If data is written correctly commpletion markers are created.
    """

    location: DataLocation
    expected_output: ExpectedOutput
    markers_table: str
    force: bool

    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        raise NotImplementedError()

    def write(self, output_data: Any):
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
            return

        written_parts = self.write_implementation(output_data)

        client.write_marker(
            data_location=self.location,
            expected_output=self.expected_output,
            written_parts=written_parts,
            markers_table=self.markers_table,
        )
        log.info(f"Wrote {self.expected_output.dataset_name} to {self.location.name}")


class ParqueWriteManager(WriteManager):
    @override
    def write_implementation(self, output_data: Any) -> list[OutputPartMeta]:
        assert isinstance(output_data, OutputData)

        client = init_data_access()

        parts = breakout_partitions(
            df=output_data.dataframe,
            partition_cols=["chain", "dt"],
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
