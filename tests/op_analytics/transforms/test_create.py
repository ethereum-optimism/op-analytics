import os


from op_analytics.coreutils.clickhouse.ddl import ClickHouseDDL, read_ddls
from op_analytics.coreutils.logger import structlog
from op_analytics.transforms.create import DIRECTORY, CreateStatement

log = structlog.get_logger()


def test_create_statement():
    group_name = "interop"
    ddls: list[ClickHouseDDL] = read_ddls(
        directory=os.path.join(DIRECTORY, group_name, "create"),
        globstr="*.sql",
    )

    assert len(ddls) == 2

    create = CreateStatement.of(
        group_name=group_name,
        ddl=ddls[0],
    )

    assert create == CreateStatement(
        db_name="transforms_interop",
        table_name="export_fact_erc20_create_traces_v1",
        statement="-- Since this table is meant to be exported we use BigQuery-compatible data types.\n\nCREATE TABLE IF NOT EXISTS transforms_interop.export_fact_erc20_create_traces_v1\n(\n    `dt` Date,\n    `chain` String,\n    `chain_id` Int32,\n    `network` String,\n    `block_timestamp` DateTime,\n    `block_number` Int64,\n    `transaction_hash` String,\n    `transaction_index` Int64,\n    `tr_from_address` String,\n    `tx_from_address` String,\n    `contract_address`String,\n    `tx_to_address` String,\n    `trace_address` String,\n    `trace_type` String,\n    `gas` String,\n    `gas_used` String,\n    `value` UInt256,\n    `code` String,\n    `call_type` String,\n    `reward_type` String,\n    `subtraces` Int64,\n    `error` String,\n    `status` Int64,\n    `tx_method_id` String,\n    `code_bytelength` Int64,\n    `is_erc7802` Bool,\n    `has_oft_events` Bool,\n    `has_ntt_events` Bool,\n    INDEX dt_idx dt TYPE minmax GRANULARITY 1,\n    INDEX chain_idx chain TYPE minmax GRANULARITY 1,\n)\nENGINE = ReplacingMergeTree\nORDER BY (dt, chain, chain_id, network, block_number, transaction_hash, transaction_index, trace_address)\n",
    )
