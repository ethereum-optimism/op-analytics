from dataclasses import dataclass
from datetime import date

from overrides import override

from op_analytics.coreutils.bigquery.load import load_from_parquet_uris
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.partition import Partition, PartitionMetadata, WrittenParts
from op_analytics.coreutils.partitioned.writehelper import WriteManager

log = structlog.get_logger()


@dataclass
class BQOutputData:
    # Root path does not include the gs://{BUCKET}/ prefix
    root_path: str

    # URIs include the gs://{BUCKET}/ prefix
    source_uris: list[str]
    source_uris_root_path: str

    dateval: date
    bq_dataset_name: str
    bq_table_name: str

    @property
    def default_partition(self) -> dict[str, str] | None:
        return None


class BQLoader(WriteManager):
    @override
    def write_implementation(self, output_data: BQOutputData) -> WrittenParts:
        assert isinstance(output_data, BQOutputData)

        num_parquet = len(output_data.source_uris)
        bq_destination = f"{output_data.bq_dataset_name}.{output_data.bq_table_name}"
        log.info(f"BEGIN Loading {num_parquet} files to {bq_destination}")

        load_from_parquet_uris(
            source_uris=output_data.source_uris,
            source_uri_prefix=output_data.source_uris_root_path + "{chain:STRING}/{dt:DATE}",
            destination=bq_destination,
            date_partition=output_data.dateval,
            time_partition_field="dt",
            clustering_fields=["chain"],
        )

        log.info(f"DONE Loading {num_parquet} files to {bq_destination}")

        return {
            # Not the actual row count, but the number of paths loaded.
            Partition.from_tuples(
                [("dt", output_data.dateval.strftime("%Y-%m-%d"))]
            ): PartitionMetadata(row_count=num_parquet)
        }
