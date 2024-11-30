"""
BLOCKS SCHEMA
"""

from textwrap import dedent

import pyarrow as pa

from op_analytics.datapipeline.schemas import shared
from op_analytics.datapipeline.schemas.core import Column, JsonRPCMethod, CoreDataset

BLOCKS_V1_SCHEMA = CoreDataset(
    name="blocks",
    goldsky_table_suffix="blocks",
    block_number_col="number",
    doc=dedent("""Indexed Blocks. See [eth_getblockbyhash](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getblockbyhash) for more info.
    
    Fields from RPC not included here:
    - parentBeaconBlockRoot
    - transactions
    - uncles
    - withdrawals
    - blobGasUsed
    - excessBlobGas
    - mixHash
    
    Field included that always have the same values:
    - nonce: 0x0000000000000000
    - sha3_uncles: 0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347
    
    """),
    columns=[
        shared.METADATA,
        shared.CHAIN,
        shared.NETWORK,
        shared.CHAIN_ID,
        # Block columns
        Column(
            name="dt",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=None,
            json_rpc_field_name=None,
            raw_goldsky_pipeline_expr=None,
            raw_goldsky_pipeline_type=None,
            op_analytics_clickhouse_expr="formatDateTime(timestamp, '%Y-%m-%d') AS dt",
        ),
        Column(
            name="timestamp",
            field_type=pa.timestamp(unit="us"),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="timestamp",
            raw_goldsky_pipeline_expr="timestamp",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="timestamp",
        ),
        Column(
            name="number",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="number",
            raw_goldsky_pipeline_expr="number",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="accurateCast(number, 'Int64') AS number",
        ),
        Column(
            name="hash",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="hash",
            raw_goldsky_pipeline_expr="hash",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="cast(hash, 'String') AS hash",
        ),
        Column(
            name="parent_hash",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="parentHash",
            raw_goldsky_pipeline_expr="parent_hash",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="parent_hash",
        ),
        Column(
            name="nonce",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="nonce",
            doc="Hash of the generated proof-of-work",
            raw_goldsky_pipeline_expr="nonce",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="nonce",
        ),
        Column(
            name="sha3_uncles",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="sha3Uncles",
            raw_goldsky_pipeline_expr="sha3_uncles",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="sha3_uncles",
        ),
        Column(
            name="logs_bloom",
            field_type=pa.large_string(),
            required=True,
            doc="Logs Bloom can be used for efficient lookup of logs from this block.",
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="logsBloom",
            raw_goldsky_pipeline_expr="logs_bloom",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="logs_bloom",
        ),
        Column(
            name="transactions_root",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="transactionsRoot",
            raw_goldsky_pipeline_expr="transactions_root",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="transactions_root",
        ),
        Column(
            name="state_root",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="stateRoot",
            raw_goldsky_pipeline_expr="state_root",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="state_root",
        ),
        Column(
            name="receipts_root",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="receiptsRoot",
            raw_goldsky_pipeline_expr="receipts_root",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="receipts_root",
        ),
        Column(
            name="withdrawals_root",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="withdrawalsRoot",
            raw_goldsky_pipeline_expr="withdrawals_root",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="withdrawals_root",
        ),
        Column(
            name="miner",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="miner",
            raw_goldsky_pipeline_expr="miner",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="miner",
        ),
        Column(
            name="difficulty",
            field_type=pa.float64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="difficulty",
            raw_goldsky_pipeline_expr="difficulty",
            raw_goldsky_pipeline_type="double",
            # NOTE: Some goldsky tables have difficulty as Decimal(76, 18) so we cast it to Float64.
            op_analytics_clickhouse_expr="cast(difficulty, 'Float64') AS difficulty",
        ),
        Column(
            name="total_difficulty",
            field_type=pa.float64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="totalDifficulty",
            raw_goldsky_pipeline_expr="total_difficulty",
            raw_goldsky_pipeline_type="double",
            # NOTE: Some goldsky tables have total_difficulty as Decimal(76, 18) so we cast it to Float64.
            op_analytics_clickhouse_expr="cast(total_difficulty, 'Float64') AS total_difficulty",
        ),
        Column(
            name="size",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="size",
            raw_goldsky_pipeline_expr="size",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="size",
        ),
        Column(
            name="base_fee_per_gas",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="baseFeePerGas",
            raw_goldsky_pipeline_expr="base_fee_per_gas",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="base_fee_per_gas",
        ),
        Column(
            name="gas_used",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="gasUsed",
            raw_goldsky_pipeline_expr="gas_used",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="gas_used",
        ),
        Column(
            name="gas_limit",
            field_type=pa.int64(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="gasLimit",
            raw_goldsky_pipeline_expr="gas_limit",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="gas_limit",
        ),
        Column(
            name="extra_data",
            field_type=pa.large_string(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="extraData",
            raw_goldsky_pipeline_expr="extra_data",
            raw_goldsky_pipeline_type="string",
            op_analytics_clickhouse_expr="extra_data",
        ),
        Column(
            name="transaction_count",
            field_type=pa.int64(),
            required=True,
            raw_goldsky_pipeline_expr="transaction_count",
            raw_goldsky_pipeline_type="long",
            op_analytics_clickhouse_expr="transaction_count",
        ),
    ],
)
