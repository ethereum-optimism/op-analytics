"""
LOGS SCHEMA
"""

from textwrap import dedent

from pyiceberg.types import LongType, StringType, TimestampType, ListType

from op_datasets.schemas import shared
from op_datasets.schemas.core import Column, JsonRPCMethod, CoreDataset

LOGS_V1_SCHEMA = CoreDataset(
    name="logs_v1",
    goldsky_table="logs",
    block_number_col="block_number",
    doc=dedent("""Indexed Logs. See [eth_getlogs](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getlogs) for more info.
    
    for more information on destructuring EVENT_INDEXED_ARGS and EVENT_NON_INDEXED_ARGS refer to
    https://docs.soliditylang.org/en/latest/abi-spec.html#events
    
    """),
    columns=[
        shared.METADATA(field_id=1),
        shared.CHAIN(field_id=2),
        shared.NETWORK(field_id=3),
        shared.CHAIN_ID(field_id=4),
        # Block
        Column(
            field_id=5,
            name="block_timestamp",
            field_type=TimestampType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="timestamp",
            raw_goldsky_pipeline_expr="block_timestamp",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="block_timestamp",
        ),
        Column(
            field_id=6,
            name="block_number",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="number",
            raw_goldsky_pipeline_expr="block_number",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(block_number, 'Int64') AS block_number",
        ),
        Column(
            field_id=7,
            name="block_hash",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="block_hash",
            raw_goldsky_pipeline_expr="block_hash",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(block_hash, 'String') AS block_hash",
        ),
        # Transaction
        Column(
            field_id=8,
            name="transaction_hash",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="transaction_hash",
            raw_goldsky_pipeline_expr="transaction_hash",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(transaction_hash, 'String') AS transaction_hash",
        ),
        Column(
            field_id=9,
            name="transaction_index",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="transaction_index",
            raw_goldsky_pipeline_expr="transaction_index",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(transaction_index, 'Int64') AS transaction_index",
        ),
        # Logs
        Column(
            field_id=10,
            name="log_index",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="hash",
            raw_goldsky_pipeline_expr="transaction_index",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(log_index, 'Int64') AS log_index",
        ),
        Column(
            field_id=11,
            name="address",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="address",
            raw_goldsky_pipeline_expr="address",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(address, 'String') AS address",
        ),
        Column(
            field_id=12,
            name="topics",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="topics",
            raw_goldsky_pipeline_expr="topics",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(topics, 'String') AS topics",
        ),
        Column(
            field_id=13,
            name="data",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="data",
            raw_goldsky_pipeline_expr="data",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(data, 'String') AS data",
        ),
        # Destructured Args
        Column(
            field_id=14,
            name="topic0",
            field_type=StringType(),
            required=True,
            json_rpc_method=None,
            json_rpc_field_name=None,
            raw_goldsky_pipeline_expr=None,
            raw_goldsky_pipeline_type=None,
            op_analytics_clickhouse_expr="splitByChar(',', topics)[1] as topic0",
        ),
        Column(
            field_id=15,
            name="indexed_args",
            doc="Event Indexed Args can be easily destructured in SQL because they are guaranteed to be fixed size (non-dynamic) types.",
            field_type=ListType(element_id=1, element_type=StringType(), element_required=True),
            required=True,
            json_rpc_method=None,
            json_rpc_field_name=None,
            raw_goldsky_pipeline_expr=None,
            raw_goldsky_pipeline_type=None,
            op_analytics_clickhouse_expr="arraySlice(splitByChar(',', topics), 2) as indexed_args",
        ),
    ],
)
