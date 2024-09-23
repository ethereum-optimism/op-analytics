"""
Schema for the "blocks" table.

The schema defined here was copied over from our current Clickhouse table schema.
This file is being commited to the repo as a source of truth for what we expect the
schema to look like on our raw "blocks" data source.

For reference here is the CREATE TABLE statement from Clickhouse:

CREATE TABLE default.base_blocks
(
    `id` String,
    `number` UInt64 CODEC(Delta(8), ZSTD(1)),
    `hash` FixedString(66),
    `parent_hash` String,
    `nonce` String,
    `sha3_uncles` String,
    `logs_bloom` String,
    `transactions_root` String,
    `state_root` String,
    `receipts_root` String,
    `miner` String,
    `difficulty` Float64,
    `total_difficulty` Float64,
    `size` Int64,
    `extra_data` String,
    `gas_limit` Int64,
    `gas_used` Int64,
    `timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `transaction_count` Int64,
    `base_fee_per_gas` Int64,
    `withdrawals_root` String,
    `chain` String,
    `network` String,
    `chain_id` Int32,
    `insert_time` DateTime,
    `is_deleted` UInt8,
    INDEX timestamp_idx timestamp TYPE minmax GRANULARITY 1,
    INDEX hash_idx hash TYPE bloom_filter GRANULARITY 1
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', insert_time, is_deleted)
ORDER BY (number, id)
SETTINGS index_granularity = 8192, min_age_to_force_merge_seconds = 1200, min_age_to_force_merge_on_partition_only = true

"""

from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import DoubleType, IntegerType, LongType, StringType, TimestampType

from op_indexer.schemas.ingestion import INGESTION_METADATA

BLOCKS_SCHEMA = Schema(
    #
    # `id` String,
    NestedField(field_id=1, name="id", field_type=StringType(), required=True),
    #
    # `number` UInt64 CODEC(Delta(8), ZSTD(1)),
    NestedField(field_id=2, name="number", field_type=LongType(), required=True),
    #
    # `hash` FixedString(66),
    NestedField(field_id=3, name="hash", field_type=StringType()),
    #
    # `parent_hash` String,
    NestedField(field_id=4, name="parent_hash", field_type=StringType()),
    #
    # `nonce` String,
    NestedField(field_id=5, name="nonce", field_type=StringType()),
    #
    # `sha3_uncles` String,
    NestedField(field_id=6, name="sha3_uncles", field_type=StringType()),
    #
    # `logs_bloom` String,
    NestedField(field_id=7, name="logs_bloom", field_type=StringType()),
    #
    # `transactions_root` String,
    NestedField(field_id=8, name="transactions_root", field_type=StringType()),
    #
    # `state_root` String,
    NestedField(field_id=9, name="state_root", field_type=StringType()),
    #
    # `receipts_root` String,
    NestedField(field_id=10, name="receipts_root", field_type=StringType()),
    #
    # `miner` String,
    NestedField(field_id=11, name="miner", field_type=StringType()),
    #
    # `difficulty` Float64,
    NestedField(field_id=12, name="difficulty", field_type=DoubleType()),
    #
    # `total_difficulty` Float64,
    NestedField(field_id=12, name="total_difficulty", field_type=DoubleType()),
    #
    # `size` Int64,
    NestedField(field_id=13, name="size", field_type=LongType()),
    #
    # `extra_data` String,
    NestedField(field_id=14, name="extra_data", field_type=StringType()),
    #
    # `gas_limit` Int64,
    NestedField(field_id=15, name="gas_limit", field_type=LongType()),
    #
    # `gas_used` Int64,
    NestedField(field_id=16, name="gas_used", field_type=LongType()),
    #
    # `timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    NestedField(field_id=17, name="timestamp", field_type=TimestampType()),
    #
    # `transaction_count` Int64,
    NestedField(field_id=18, name="transaction_count", field_type=LongType()),
    #
    # `base_fee_per_gas` Int64,
    NestedField(field_id=19, name="base_fee_per_gas", field_type=LongType()),
    #
    # `withdrawals_root` String,
    NestedField(field_id=20, name="withdrawals_root", field_type=StringType()),
    #
    # `chain` String,
    NestedField(field_id=21, name="chain", field_type=StringType()),
    #
    # `network` String,
    NestedField(field_id=22, name="network", field_type=StringType()),
    #
    # `chain_id` Int32,
    NestedField(field_id=23, name="chain_id", field_type=IntegerType()),
    #
    INGESTION_METADATA,
)
