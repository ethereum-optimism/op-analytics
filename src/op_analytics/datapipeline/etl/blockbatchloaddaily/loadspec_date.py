from dataclasses import dataclass, field

from op_analytics.coreutils.rangeutils.daterange import DateRange

from .loadspec_datechain import ETLMixin
from .markers import query_blockbatch_daily_markers
from .readers import DateChainBatch


@dataclass
class ClickHouseDateETL(ETLMixin):
    """Represent a task to load data to ClickHouse by dt.


    A ClickHouseDateETL is defined by a SQL query that reads data for a given dt
    for all chains and, transforms it and writes the result to ClickHouse.
    """

    # Output root path determines the ClickHouse table name where data will be loaded.
    output_root_path: str

    # This is the list of blockbatch root paths that are inputs to this load task.
    inputs_blockbatch: list[str] = field(default_factory=list)

    # This is the list of ClickHouse root paths that are inputs to this load task.
    inputs_clickhouse: list[str] = field(default_factory=list)

    def insert_ddl_template(self, batch: DateBatch, dry_run: bool = False):
        select_ddl = self.read_insert_ddl()

        # If needed for debugging we can log out the DDL template
        # log.info(ddl)

        select_ddl = self.replace_inputs_blockbatch(select_ddl, batch, dry_run)
        select_ddl = self.replace_inputs_clickhouse(select_ddl, batch, dry_run)

        output_table = self.output_table_name()
        insert_ddl = f"INSERT INTO {output_table}\n" + select_ddl

        return insert_ddl

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
            WHERE 
                dt = '{batch.dt}'
            )
            """

            placeholder = f"INPUT_CLICKHOUSE('{input_root_path}')"
            select_ddl = select_ddl.replace(placeholder, subquery)

        return select_ddl

    def existing_markers(self, range_spec: str) -> set[DateChainBatch]:
        # Existing markers that have already been loaded to ClickHouse.
        date_range = DateRange.from_spec(range_spec)
        existing_markers_df = query_blockbatch_daily_markers(
            date_range=date_range,
            chains=["ALL"],
            root_paths=[self.output_root_path],
        )
        return set(
            DateChainBatch.of(chain=x["chain"], dt=x["dt"]) for x in existing_markers_df.to_dicts()
        )
