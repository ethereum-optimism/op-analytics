from dataclasses import dataclass
from datetime import date

import pyarrow as pa
from overrides import override

from op_analytics.coreutils.bigquery.load import load_from_parquet_uris
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import Partition, PartitionMetadata, WrittenParts
from op_analytics.coreutils.partitioned.writehelper import WriteManager, WriteResult

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


def bq_load(
    location: DataLocation,
    dateval: date,
    bq_dataset_name: str,
    bq_table_name: str,
    markers_table: str,
    source_uris: list[str],
    source_uris_root_path: str,
    force_complete: bool,
) -> WriteResult:
    """Use a WriteManager class to handle writing completion markers."""
    location.ensure_biguqery()

    bq_root_path = f"{bq_dataset_name}/{bq_table_name}"

    manager = BQLoader(
        location=location,
        expected_outputs=[
            ExpectedOutput(
                root_path=bq_root_path,
                file_name="",  # Not meaningful for BQ Load
                marker_path=f"{bq_dataset_name}/{bq_table_name}/{dateval.strftime("%Y-%m-%d")}",
                process_name="default",
                additional_columns={},
                additional_columns_schema=[
                    pa.field("dt", pa.date32()),
                ],
            )
        ],
        markers_table=markers_table,
        force=force_complete,
    )

    return manager.write(
        output_data=BQOutputData(
            root_path=bq_root_path,
            source_uris=source_uris,
            source_uris_root_path=source_uris_root_path,
            dateval=dateval,
            bq_dataset_name=bq_dataset_name,
            bq_table_name=bq_table_name,
        )
    )
