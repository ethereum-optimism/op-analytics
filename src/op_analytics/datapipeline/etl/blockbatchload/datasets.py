from .loadspec import ClickHouseBlockBatchETL


CONTRACT_CREATION = ClickHouseBlockBatchETL(
    input_root_paths=["blockbatch/contract_creation/create_traces_v1"],
    output_root_path="blockbatch/contract_creation/create_traces_v1",
    enforce_non_zero_row_count=True,
)

ERC20_TRANSFERS = ClickHouseBlockBatchETL(
    input_root_paths=["blockbatch/token_transfers/erc20_transfers_v1"],
    output_root_path="blockbatch/token_transfers/erc20_transfers_v1",
    # We don't enforce row count for this dataset because the INSERT sql
    # includes a filter to include only rows where amount_lossless is not NULL.
    enforce_non_zero_row_count=False,
)

ERC721_TRANSFERS = ClickHouseBlockBatchETL(
    input_root_paths=["blockbatch/token_transfers/erc721_transfers_v1"],
    output_root_path="blockbatch/token_transfers/erc721_transfers_v1",
    enforce_non_zero_row_count=True,
)

NATIVE_TRANSFERS = ClickHouseBlockBatchETL(
    input_root_paths=["blockbatch/native_transfers/native_transfers_v1"],
    output_root_path="blockbatch/native_transfers_v1/native_transfers_v1",
    enforce_non_zero_row_count=True,
)

REVSHARE_TRANSFERS = ClickHouseBlockBatchETL(
    input_root_paths=[
        "blockbatch/native_transfers/native_transfers_v1",
        "blockbatch/token_transfers/erc20_transfers_v1",
    ],
    output_root_path="blockbatch/revshare_transfers/revshare_transfers_v1",
    enforce_non_zero_row_count=True,
)
