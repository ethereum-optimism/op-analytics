from dataclasses import dataclass

from op_analytics.coreutils.clickhouse.oplabs import run_query_oplabs
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

from .dataaccess import Agora


@dataclass
class AgoraSchema:
    table: DailyDataset
    gcs_path: str
    schema: dict[str, str]
    order_by: list[str]
    auto_dt: bool = False

    def table_name(self):
        return f"transforms_governance.ingest_{self.table.table}"

    def schema_type(self, name: str, dtype: str):
        """Return the type of a column for use in CREATE and SELECT statements."""
        if name in self.order_by:
            return dtype
        else:
            return f"Nullable({dtype})"

    def select(self, filter_clause: str = ""):
        """Return a SELECT statement to read from the Agora public bucket."""

        schema_cols = [
            f"CAST({name} AS {self.schema_type(name, dtype)}) AS {name}"
            for name, dtype in self.schema.items()
        ]

        if self.auto_dt:
            cols = [
                "toDate(now()) AS dt",
                *schema_cols,
            ]
        else:
            cols = schema_cols

        columns = "\n, ".join(cols)

        return f"""
        SELECT
            {columns}
        FROM s3(
            'https://storage.googleapis.com/agora-optimism-public-usw1/v1/snapshot/{self.gcs_path}',
            'CSVWithNames'
        )
        {filter_clause}
        """

    def create(self):
        """Return a CREATE statement to create the table in the OP Labs ClickHouse instancd."""

        cols = [f"`{name}` {self.schema_type(name, dtype)}" for name, dtype in self.schema.items()]

        if self.auto_dt:
            create_cols = [
                "`dt` Date",
                *cols,
            ]
        else:
            create_cols = cols

        indexes = []
        if self.auto_dt:
            indexes.append("INDEX dt_idx dt TYPE minmax GRANULARITY 1")

        if "block_number" in self.schema:
            indexes.append("INDEX block_number_idx block_number TYPE minmax GRANULARITY 1")

        columns = ",\n".join(create_cols + indexes)
        order_by = ", ".join(self.order_by)

        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name()}
        (
            {columns}
        )
        ENGINE = ReplacingMergeTree
        ORDER BY ({order_by})
        """

    def incremental_filter_clause(self):
        """Return a filter clause to read from the Agora public bucket.

        This is used for incremental ingestion. The filter uses the maximum value of
        the block_number column present in the OPL Labs ClickHouse instance.
        """
        if self.auto_dt:
            return ""

        df = run_query_oplabs(
            f"SELECT max(block_number) as max_block_number FROM {self.table_name()}"
        )
        if len(df) == 0:
            return ""

        max_block_number = df.to_dicts()[0]["max_block_number"]
        return f"WHERE block_number > {max_block_number}"


OVERWRITE_SCHEMAS = [
    #
    AgoraSchema(
        table=Agora.PROPOSALS,
        gcs_path="proposals_v2",
        schema={
            "proposal_id": "String",
            "contract": "String",
            "proposer": "String",
            "description": "String",
            "ordinal": "Int64",
            "created_block": "Int64",
            "start_block": "Int64",
            "end_block": "Int64",
            "queued_block": "Int64",
            "cancelled_block": "Int64",
            "executed_block": "Int64",
            "proposal_data": "String",
            "proposal_data_raw": "String",
            "proposal_type": "String",
            "proposal_type_data": "String",
            "proposal_results": "String",
            "created_transaction_hash": "String",
            "cancelled_transaction_hash": "String",
            "queued_transaction_hash": "String",
            "executed_transaction_hash": "String",
            "proposal_type_id": "Int64",
        },
        order_by=[
            "proposal_id",
        ],
        auto_dt=True,
    ),
    #
    AgoraSchema(
        table=Agora.DELEGATES,
        gcs_path="delegates",
        schema={
            "delegate": "String",
            "num_of_delegators": "Int64",
            "direct_vp": "String",
            "advanced_vp": "String",
            "voting_power": "String",
            "contract": "String",
        },
        order_by=[
            "delegate",
        ],
        auto_dt=True,
    ),
]

INCREMENTAL_SCHEMAS = [
    AgoraSchema(
        table=Agora.VOTING_POWER_SNAPS,
        gcs_path="voting_power_snaps",
        schema={
            "id": "String",
            "delegate": "String",
            "balance": "String",
            "block_number": "Int64",
            "ordinal": "Int64",
            "transaction_index": "Int64",
            "log_index": "Int64",
            "contract": "String",
        },
        order_by=[
            "id",
            "block_number",
            "transaction_index",
            "log_index",
        ],
    ),
    #
    AgoraSchema(
        table=Agora.VOTES,
        gcs_path="votes",
        schema={
            "transaction_hash": "String",
            "proposal_id": "String",
            "voter": "String",
            "support": "Int64",
            "weight": "String",
            "reason": "String",
            "block_number": "Int64",
            "params": "String",
            "start_block": "Int64",
            "description": "String",
            "proposal_data": "String",
            "proposal_type": "String",
            "contract": "String",
            "chain_id": "Int64",
        },
        order_by=[
            "block_number",
            "transaction_hash",
            "proposal_id",
        ],
    ),
    #
    AgoraSchema(
        table=Agora.DELEGATE_CHANGED_EVENTS,
        gcs_path="delegate_changed_events",
        schema={
            "chain_id": "Int64",
            "address": "String",
            "block_number": "Int64",
            "block_hash": "String",
            "log_index": "Int64",
            "transaction_index": "Int64",
            "transaction_hash": "String",
            "delegator": "String",
            "from_delegate": "String",
            "to_delegate": "String",
        },
        order_by=[
            "chain_id",
            "address",
            "block_number",
            "log_index",
            "transaction_index",
        ],
    ),
]
