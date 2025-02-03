from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.clickhouse.ddl import read_ddls


def test_read_ddls():
    # Get the path to the transforms directory
    transforms_dir = repo_path("src/op_analytics/transforms")
    assert isinstance(transforms_dir, str)

    # Read all SQL files in the ddl directory
    ddls = read_ddls(transforms_dir, "ddl/dim_first_erc20_transfers_v1/*UPDATE*.sql")

    # Verify we got some DDL files back
    assert len(ddls) == 2

    # Verify each DDL is a non-empty string
    paths = [_.relative_path for _ in ddls]
    assert paths == [
        "dim_first_erc20_transfers_v1/UPDATE_01.sql",
        "dim_first_erc20_transfers_v1/UPDATE_02.sql",
    ]
