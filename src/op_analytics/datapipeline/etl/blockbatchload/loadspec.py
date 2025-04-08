import os
from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.clickhouse.ddl import read_ddl
from op_analytics.coreutils.clickhouse.inferschema import parquet_to_subquery
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


@dataclass
class BlockBatch:
    """Represent a blockbatch that needs to be loaded into ClickHouse."""

    chain: str
    dt: date
    min_block: int
    partitioned_path: str


@dataclass
class ClickHouseBlockBatchDataset:
    """The input datasets and output dataset associated with a load into ClickHouse task."""

    # This is the list of blockbatch root paths that are inputs to this load task.
    input_root_paths: list[str]

    # Output root path determiens the ClickHouse table name where data will be loaded.
    output_root_path: str

    # This flag determines if the row count will be enforced.
    enforce_non_zero_row_count: bool = False

    @staticmethod
    def sanitize_root_path(root_path: str):
        """Use the first part of the root path as the ClickHouse db and the rest as the table."""

        prefix, suffix = root_path.split("/", maxsplit=1)
        return f"{prefix}.{suffix.replace('/', '__')}"

    @property
    def ddl_path(self):
        _, ddl_path = self.output_root_path.split("/", maxsplit=1)
        return f"ddl/{ddl_path}"

    def output_table_name(self):
        return self.sanitize_root_path(self.output_root_path)

    def read_insert_ddl(self):
        return read_ddl(
            path=os.path.join(DIRECTORY, f"{self.ddl_path}__INSERT.sql"),
        )

    def create_table(self):
        create_ddl = read_ddl(
            path=os.path.join(DIRECTORY, f"{self.ddl_path}__CREATE.sql"),
        )

        create_ddl = create_ddl.replace("_placeholder_", self.output_table_name())

        log.info(f"CREATE TABLE {self.output_table_name()}")
        run_statememt_oplabs(statement=create_ddl)

    def insert_ddl_template(self, blockbatch: BlockBatch, dry_run: bool = False):
        select_ddl = self.read_insert_ddl()

        # If needed for debugging we can log out the DDL template
        # log.info(ddl)

        # Replace the input tables in the template with s3() table functions.
        for input_root_path in self.input_root_paths:
            placeholder = f"INPUT_BLOCKBATCH('{input_root_path}')"

            input_s3 = parquet_to_subquery(
                gcs_parquet_path=f"gs://oplabs-tools-data-sink/{input_root_path}/{blockbatch.partitioned_path}",
                dry_run=dry_run,
            )
            select_ddl = select_ddl.replace(placeholder, input_s3)

        output_table = self.output_table_name()
        insert_ddl = f"INSERT INTO {output_table}\n" + select_ddl

        # The INSERT sql file  supports the `BLOCKBATCH_MIN_BLOCK()` placeholder which is
        # replaced with the minimum block number of the blockbatch being processed. This can
        # be used to keep track of the blockbatch that produced some data in the output table.
        #
        # An example of where this can be useful is a partial aggregation table where the
        # batch aggregate can be much less data that all the rows in the batch, and the
        # aggregation state can be reaggregated at a more meaningful grain afterwards.
        #
        # To be more concrete, imagine a table like:
        #
        # CREATE TABLE blockbatch_uniq_trace_from_addresses (
        #     dt Date,
        #     chain String,
        #     batch_min_block UInt32,
        #     count_distinct_trace_from AggregateFunction(uniq, Nullable(String))
        # ) ENGINE = MergeTree() ORDER BY (dt, chain, batch_min_block);
        #
        # If we process a batch more than once we want to make sure that the result is
        # deduplicated at the batch level, so including the batch_min_block in the ORDER BY
        # is critical.
        #
        # The INSERT sql file will look like:
        #
        # SELECT
        #     dt,
        #     chain,
        #     BLOCKBATCH_MIN_BLOCK() as batch_min_block,
        #     uniq(trace_from_address)
        # FROM input_blockbatch('blockbatch/traces')
        # GROUP BY dt, chain, batch_min_block;
        #
        # The `BLOCKBATCH_MIN_BLOCK()` placeholder is replaced with the minimum block number
        # of the blockbatch being processed.

        insert_ddl = insert_ddl.replace(
            "BLOCKBATCH_MIN_BLOCK()",
            str(blockbatch.min_block),
        )

        return insert_ddl
