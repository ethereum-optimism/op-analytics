from dataclasses import dataclass

import duckdb
from overrides import override

from op_analytics.coreutils.logger import structlog, human_rows, human_size, memory_usage
from op_analytics.coreutils.storage.gcs_parquet import BUCKET_NAME
from op_analytics.coreutils.duckdb_inmem.client import init_client

from .partition import WrittenParts, PartitionMetadata, Partition
from .writemanager import WriteManager

log = structlog.get_logger()


@dataclass
class OutputDuckDBRelation:
    relation: duckdb.DuckDBPyRelation

    # Root path
    root_path: str

    # Partitions
    partition: Partition

    def gcs_path(self, full_path: str) -> str:
        return f"gs://{BUCKET_NAME}/{full_path}"

    @property
    def default_partitions(self) -> list[dict[str, str]]:
        """Provided for logging.

        The WriteManager class adds default_partitions() to the logging context.
        """
        return [self.partition.as_dict()]

    def without_partition_cols(self) -> duckdb.DuckDBPyRelation:
        exclude = ", ".join(self.partition.col_names())
        return self.relation.select(f"* EXCLUDE ({exclude})")


@dataclass
class DuckDBWriteManager(WriteManager):
    """Write a duckdb relation to a single parquet file."""

    @override
    def write_implementation(self, output_data: OutputDuckDBRelation) -> WrittenParts:
        assert isinstance(output_data, OutputDuckDBRelation)
        eo = self.expected_output(output_data)

        # Construct the output path.
        self.location.check_write_allowed()
        assert output_data.root_path == eo.root_path
        relative_path = output_data.partition.full_path(
            root_path=eo.root_path,
            file_name=eo.file_name,
        )

        # Write the data.
        abs_path = self.location.absolute(relative_path)
        self.location.create_dir(relative_path)
        output_data.without_partition_cols().write_parquet(file_name=abs_path)
        log.info(f"wrote parquet file at {abs_path}", max_rss=memory_usage())

        # Verify the written data.
        written = {}
        client = init_client().client

        file_size = client.sql(f"""
        SELECT sum(total_compressed_size)
        FROM parquet_metadata('{abs_path}')
        """).fetchone()[0]  # type: ignore

        num_rows = client.sql(f"""
        SELECT num_rows
        FROM parquet_file_metadata('{abs_path}')
        """).fetchone()[0]  # type: ignore

        log.info(
            f"verified parquet file {human_rows(num_rows)} {human_size(file_size)}",
            max_rss=memory_usage(),
        )
        written[output_data.partition] = PartitionMetadata(row_count=num_rows)

        return written
