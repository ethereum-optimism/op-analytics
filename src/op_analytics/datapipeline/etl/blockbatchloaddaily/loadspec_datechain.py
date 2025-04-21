import os
from dataclasses import dataclass, field

from op_analytics.coreutils.clickhouse.ddl import read_ddl
from op_analytics.coreutils.clickhouse.inferschema import parquet_to_subquery
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains

from .markers import query_blockbatch_daily_markers
from .readers import DateChainBatch, construct_batches, ALL_CHAINS_SENTINEL


log = structlog.get_logger()


DIRECTORY = os.path.dirname(__file__)


class ETLMixin:
    """Shared functionality for the DateChain and Date ETL classes"""

    output_root_path: str
    inputs_blockbatch: list[str]
    inputs_clickhouse: list[str]

    def __post_init__(self):
        for root_path in self.inputs_blockbatch:
            if not root_path.startswith("blockbatch/"):
                raise ValueError(f"Invalid blockbatch input: {root_path}")

        for root_path in self.inputs_clickhouse:
            # Check that the patch can be sanitized.
            self.sanitize_root_path(root_path)

        # Check that the output root path can be sanitized.
        self.sanitize_root_path(self.output_root_path)

    @staticmethod
    def sanitize_root_path(root_path: str):
        """Use the first part of the root path as the ClickHouse db and the rest as the table."""

        prefix, suffix = root_path.split("/", maxsplit=1)
        return f"{prefix}.{suffix.replace('/', '__')}"

    @property
    def ddl_path(self):
        return f"ddl/{self.output_root_path}"

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

    def validate_batch(self, batch: DateChainBatch):
        """For date level ETLs we expect ALL_CHAINS_SENTINEL."""
        if self.__class__.__name__ == "ClickHouseDateETL":
            assert batch.chain == ALL_CHAINS_SENTINEL

    def insert_ddl_template(self, batch: DateChainBatch, dry_run: bool = False):
        self.validate_batch(batch)

        select_ddl = self.read_insert_ddl()

        # If needed for debugging we can log out the DDL template
        # log.info(ddl)

        select_ddl = self.replace_inputs_blockbatch(select_ddl, batch, dry_run)
        select_ddl = self.replace_inputs_clickhouse(select_ddl, batch, dry_run)

        output_table = self.output_table_name()
        insert_ddl = f"INSERT INTO {output_table}\n" + select_ddl

        return insert_ddl

    def replace_inputs_blockbatch(
        self,
        select_ddl: str,
        batch: DateChainBatch,
        dry_run: bool = False,
    ):
        self.validate_batch(batch)

        for input_root_path in self.inputs_blockbatch:
            # Prepare the s3 table function to read data from GCS.
            input_path = f"gs://oplabs-tools-data-sink/{input_root_path}/{batch.partitioned_path}"
            input_s3 = parquet_to_subquery(gcs_parquet_path=input_path, dry_run=dry_run)

            # Replace the input table place holder with the GCS subquery.
            placeholder = f"INPUT_BLOCKBATCH('{input_root_path}')"
            select_ddl = select_ddl.replace(placeholder, input_s3)

        return select_ddl

    def replace_inputs_clickhouse(
        self,
        select_ddl: str,
        batch: DateChainBatch,
        dry_run: bool = False,
    ):
        for input_root_path in self.inputs_clickhouse:
            # Prepare the ClickHouse subquery to read data from the input daily table.
            input_clickhouse_table_name = self.sanitize_root_path(input_root_path)

            subquery = f"""
            (
            SELECT
                * 
            FROM {input_clickhouse_table_name}
            WHERE {batch.clickhouse_filter}
            )
            """

            placeholder = f"INPUT_CLICKHOUSE('{input_root_path}')"
            select_ddl = select_ddl.replace(placeholder, subquery)

        return select_ddl

    def existing_markers(self, range_spec: str, chains: list[str]) -> set[DateChainBatch]:
        # Existing markers that have already been loaded to ClickHouse.
        date_range = DateRange.from_spec(range_spec)
        existing_markers_df = query_blockbatch_daily_markers(
            date_range=date_range,
            chains=chains,
            root_paths=[self.output_root_path],
        )
        return set(
            DateChainBatch.of(chain=x["chain"], dt=x["dt"]) for x in existing_markers_df.to_dicts()
        )


@dataclass
class ClickHouseDateChainETL(ETLMixin):
    """Represent a task to load data to ClickHouse by dt,chain.


    A ClickHouseDateChainETL is defined by a SQL query that reads data for a given dt,chain
    combination, transforms it and writes the result to ClickHouse.

    The input data can be blockbatch data stored in GCS or data already loaded into ClickHouse
    having `dt` and `chain` columns.
    """

    # Output root path determines the ClickHouse table name where data will be loaded.
    output_root_path: str

    # This is the list of blockbatch root paths that are inputs to this load task.
    inputs_blockbatch: list[str] = field(default_factory=list)

    # This is the list of ClickHouse root paths that are inputs to this load task.
    inputs_clickhouse: list[str] = field(default_factory=list)

    # This is a list of chains for which non_zero row_count will not be enforced.
    ignore_zero_rows_chains: list[str] | None = None

    # This is a list of chain,dt tuples for which non_zero row_count will not be enforced.
    ignore_zero_rows_chain_dts: list[tuple[str, str]] | None = None

    def pending_batches(
        self, range_spec: str, chains: list[str] | None = None
    ) -> list[DateChainBatch]:
        chains = chains or goldsky_mainnet_chains()

        ready_batches: list[DateChainBatch] = construct_batches(
            range_spec=range_spec,
            chains=chains,
            blockbatch_root_paths=self.inputs_blockbatch,
            clickhouse_root_paths=self.inputs_clickhouse,
        )

        # Existing markers that have already been loaded to ClickHouse.
        existing_markers = self.existing_markers(range_spec=range_spec, chains=chains)

        # Loop over batches and find which ones are pending.
        batches: list[DateChainBatch] = []
        for batch in ready_batches:
            if batch in existing_markers:
                continue
            batches.append(batch)
        log.info(f"{len(batches)}/{len(ready_batches)} pending dt,chain insert tasks.")

        return batches
