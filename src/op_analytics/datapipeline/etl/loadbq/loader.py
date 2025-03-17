from dataclasses import dataclass
from datetime import date
from overrides import override

from op_analytics.coreutils.partitioned.paths import get_dt, get_root_path
from op_analytics.coreutils.bigquery.load import load_from_parquet_uris
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.output import ExpectedOutput
from op_analytics.coreutils.partitioned.partition import Partition, PartitionMetadata, WrittenParts
from op_analytics.coreutils.partitioned.writemanager import WriteManager


log = structlog.get_logger()


@dataclass
class BQOutputData:
    # Root path does not include the gs://{BUCKET}/ prefix
    root_path: str

    # URIs include the gs://{BUCKET}/ prefix
    source_uris: list[str]
    source_uris_root_path: str

    # Date being written to BQ.
    dateval: date

    # BQ table information.
    bq_dataset_name: str
    bq_table_name: str

    # Corresponding expected output. Needed towrite markers after BQ loading is complete.
    expected_output: ExpectedOutput

    @property
    def default_partitions(self) -> list[dict[str, str]] | None:
        return None

    @staticmethod
    def get_bq_table_name(root_path: str, table_name_map: dict[str, str]) -> str:
        # Obtain the BQ table name from the root path. For example:
        # root_path = "ingestion/traces_v1"  --> traces_v1.
        bq_table_name = root_path.split("/")[-1]

        # The table_name_map overrides the default BQ table name.
        if bq_table_name in table_name_map:
            bq_table_name = table_name_map[bq_table_name]

        return bq_table_name

    @classmethod
    def get_bq_root_path(
        cls,
        root_path: str,
        table_name_map: dict[str, str],
        target_bq_dataset_name: str,
    ) -> str:
        bq_table_name = cls.get_bq_table_name(root_path=root_path, table_name_map=table_name_map)
        return f"{target_bq_dataset_name}/{bq_table_name}"

    @classmethod
    def construct(
        cls,
        root_path: str,
        parquet_paths: list[str],
        target_dateval: date,
        target_bq_dataset_name: str,
        table_name_map: dict[str, str],
    ) -> "BQOutputData":
        # Get the common root path for all the source parquet paths.
        source_uris_root_path = get_root_path(parquet_paths)
        assert source_uris_root_path.endswith(root_path + "/")

        # Get the common date partition for all the source parquet paths
        # and make sure it agrees with the task.
        dateval_from_paths = get_dt(parquet_paths)
        assert target_dateval == dateval_from_paths

        bq_table_name = cls.get_bq_table_name(
            root_path=root_path,
            table_name_map=table_name_map,
        )

        bq_root_path = cls.get_bq_root_path(
            root_path=root_path,
            table_name_map=table_name_map,
            target_bq_dataset_name=target_bq_dataset_name,
        )

        return cls(
            root_path=bq_root_path,
            source_uris=parquet_paths,
            source_uris_root_path=source_uris_root_path,
            dateval=target_dateval,
            bq_dataset_name=target_bq_dataset_name,
            bq_table_name=bq_table_name,
            expected_output=ExpectedOutput(
                root_path=bq_root_path,
                file_name="",  # Not meaningful for BQ Load
                marker_path=f"{target_bq_dataset_name}/{bq_table_name}/{target_dateval.strftime("%Y-%m-%d")}",
            ),
        )


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
