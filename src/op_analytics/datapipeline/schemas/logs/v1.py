"""
LOGS SCHEMA
"""

from textwrap import dedent

import pyarrow as pa

from op_analytics.datapipeline.schemas import shared
from op_analytics.datapipeline.schemas.core import Column, JsonRPCMethod, CoreDataset

LOGS_V1_SCHEMA = CoreDataset(
    name="logs",
    versioned_location="ingestion/logs_v1",
    goldsky_table_suffix="logs",
    block_number_col="block_number",
    doc=dedent("""Indexed Logs. See [eth_getlogs](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getlogs) for more info.
    
    for more information on destructuring EVENT_INDEXED_ARGS and EVENT_NON_INDEXED_ARGS refer to
    https://docs.soliditylang.org/en/latest/abi-spec.html#events
    
    """),
    columns=[
        shared.METADATA,
        shared.CHAIN,
        shared.NETWORK,
        shared.CHAIN_ID,
        # Block
        Column(
            name="dt",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=None,
            json_rpc_field_name=None,
            raw_goldsky_pipeline_expr=None,
            raw_goldsky_pipeline_type=None,
            op_analytics_clickhouse_expr="formatDateTime(block_timestamp, '%Y-%m-%d') AS dt",
        ),
        Column(
            name="block_timestamp",
            field_type=pa.timestamp(unit="us"),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="timestamp",
            raw_goldsky_pipeline_expr="block_timestamp",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="block_timestamp",
        ),
        Column(
            name="block_number",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="number",
            raw_goldsky_pipeline_expr="block_number",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(block_number, 'Int64') AS block_number",
        ),
        Column(
            name="block_hash",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="block_hash",
            raw_goldsky_pipeline_expr="block_hash",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(block_hash, 'String') AS block_hash",
        ),
        # Transaction
        Column(
            name="transaction_hash",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="transaction_hash",
            raw_goldsky_pipeline_expr="transaction_hash",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(transaction_hash, 'String') AS transaction_hash",
        ),
        Column(
            name="transaction_index",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="transaction_index",
            raw_goldsky_pipeline_expr="transaction_index",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(transaction_index, 'Int64') AS transaction_index",
        ),
        # Logs
        Column(
            name="log_index",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="hash",
            raw_goldsky_pipeline_expr="transaction_index",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(log_index, 'Int64') AS log_index",
        ),
        Column(
            name="address",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="address",
            raw_goldsky_pipeline_expr="address",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(address, 'String') AS address",
        ),
        Column(
            name="topics",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="topics",
            raw_goldsky_pipeline_expr="topics",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(topics, 'String') AS topics",
        ),
        Column(
            name="data",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getLogs,
            json_rpc_field_name="data",
            raw_goldsky_pipeline_expr="data",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(data, 'String') AS data",
        ),
        # Destructured Args
        Column(
            name="topic0",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=None,
            json_rpc_field_name=None,
            raw_goldsky_pipeline_expr=None,
            raw_goldsky_pipeline_type=None,
            op_analytics_clickhouse_expr="splitByChar(',', topics)[1] as topic0",
        ),
        Column(
            name="indexed_args",
            doc="Event Indexed Args can be easily destructured in SQL because they are guaranteed to be fixed size (non-dynamic) types.",
            field_type=pa.large_list(value_type=pa.large_string()),
            required=True,
            json_rpc_method=None,
            json_rpc_field_name=None,
            raw_goldsky_pipeline_expr=None,
            raw_goldsky_pipeline_type=None,
            op_analytics_clickhouse_expr="arraySlice(splitByChar(',', topics), 2) as indexed_args",
        ),
    ],
)
