from op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec import ClickHouseDateChainETL


def test_loadspec_01():
    """Use the blockbatch_daily root path."""

    dataset = ClickHouseDateChainETL(
        output_root_path="blockbatch_daily/transfers/erc20_first_seen_v1",
        inputs_blockbatch=[
            "blockbatch/token_transfers/erc20_transfers_v1",
        ],
    )

    assert dataset.output_table_name() == "blockbatch_daily.transfers__erc20_first_seen_v1"


def test_loadspec_02():
    dataset = ClickHouseDateChainETL(
        output_root_path="transforms_interop/dim_erc20_first_seen_v1",
        inputs_blockbatch=[
            "blockbatch/token_transfers/erc20_transfers_v1",
        ],
    )

    assert dataset.output_table_name() == "transforms_interop.dim_erc20_first_seen_v1"
