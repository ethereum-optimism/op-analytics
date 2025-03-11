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

    assert len(ddls) == 9

    create = CreateStatement.of(
        group_name=group_name,
        ddl=ddls[0],
    )

    assert create == CreateStatement(
        db_name="transforms_interop",
        table_name="dim_erc20_first_seen_v1",
        statement="CREATE TABLE IF NOT EXISTS transforms_interop.dim_erc20_first_seen_v1\n(\n    `chain` String,\n    `chain_id` Int32,\n    `contract_address` FixedString(66),\n    `first_seen` DateTime,\n    `row_version` Int64,\n    INDEX chain_idx chain TYPE minmax GRANULARITY 1,\n    INDEX contract_address_idx contract_address TYPE minmax GRANULARITY 1,\n)\nENGINE = ReplacingMergeTree(row_version)\nORDER BY (chain, contract_address)\n",
    )
