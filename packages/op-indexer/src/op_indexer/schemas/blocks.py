"""
BLOCKS SCHEMA
"""

from textwrap import dedent

from pyiceberg.types import (
    LongType,
    StringType,
)

from op_indexer.core import Column, Table, JsonRPCMethod
from op_indexer import shared

BLOCKS_SCHEMA = Table(
    name="blocks",
    doc=dedent("""Indexed Blocks. See [eth_getblockbyhash](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getblockbyhash) for more info.
    
    Fields from RPC not included here:
    - parentBeaconBlockRoot
    - receiptsRoot
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
        shared.INGESTION_METADATA(field_id=1),
        shared.CHAIN(field_id=2),
        shared.NETWORK(field_id=3),
        shared.CHAIN_ID(field_id=4),
        # Block columns
        Column(
            field_id=5,
            name="timestamp",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="timestamp",
        ),
        Column(
            field_id=6,
            name="number",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="number",
        ),
        Column(
            field_id=7,
            name="hash",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="hash",
        ),
        Column(
            field_id=8,
            name="parent_hash",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="parentHash",
        ),
        Column(
            field_id=9,
            name="nonce",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="nonce",
        ),
        Column(
            field_id=10,
            name="sha3_uncles",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="sha3Uncles",
        ),
        Column(
            field_id=11,
            name="logs_bloom",
            field_type=StringType(),
            required=True,
            doc="Logs Bloom can be used for efficient lookup of logs from this block.",
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="logsBloom",
        ),
        Column(
            field_id=12,
            name="transactions_root",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="transactionsRoot",
        ),
        Column(
            field_id=13,
            name="state_root",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="stateRoot",
        ),
        Column(
            field_id=14,
            name="receipts_root",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="receiptsRoot",
        ),
        Column(
            field_id=15,
            name="withdrawals_root",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="withdrawalsRoot",
        ),
        Column(
            field_id=16,
            name="miner",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="miner",
        ),
        Column(
            field_id=17,
            name="difficulty",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="difficulty",
        ),
        Column(
            field_id=18,
            name="total_difficulty",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="totalDifficulty",
        ),
        Column(
            field_id=19,
            name="size",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="size",
        ),
        Column(
            field_id=20,
            name="base_fee_per_gas",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="baseFeePerGas",
        ),
        Column(
            field_id=21,
            name="gas_used",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="gasUsed",
        ),
        Column(
            field_id=22,
            name="gas_limit",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="gasLimit",
        ),
        Column(
            field_id=23,
            name="gas_limit",
            field_type=LongType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="gasLimit",
        ),
        Column(
            field_id=24,
            name="extra_data",
            field_type=StringType(),
            required=True,
            json_rpc_method=JsonRPCMethod.eth_getBlockByNumber,
            json_rpc_field_name="extraData",
        ),
        Column(
            field_id=25,
            name="transaction_count",
            field_type=StringType(),
            required=True,
            enrichment_function="count_transactions",
        ),
    ],
)
