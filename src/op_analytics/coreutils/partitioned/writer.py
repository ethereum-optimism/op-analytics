from dataclasses import dataclass

from overrides import override

from op_analytics.coreutils.logger import structlog

from .breakout import breakout_partitions
from .dataaccess import init_data_access
from .output import OutputData
from .partition import WrittenParts, PartitionMetadata
from .writehelper import WriteManager

log = structlog.get_logger()


@dataclass
class PartitionedWriteManager(WriteManager):
    @override
    def write_implementation(self, output_data: OutputData) -> WrittenParts:
        assert isinstance(output_data, OutputData)
        expected_output = self.expected_output(output_data)

        client = init_data_access()

        parts = breakout_partitions(
            df=output_data.dataframe,
            partition_cols=self.partition_cols,
            default_partitions=output_data.default_partitions,
        )

        written = {}
        for part in parts:
            client.write_single_part(
                location=self.location,
                dataframe=part.df,
                full_path=part.partition.full_path(
                    root_path=expected_output.root_path,
                    file_name=expected_output.file_name,
                ),
            )
            written[part.partition] = PartitionMetadata(row_count=len(part.df))

        return written
