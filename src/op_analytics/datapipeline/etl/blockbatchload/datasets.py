from .insert import LoadSpec


CONTRACT_CREATION = LoadSpec.pass_through(
    root_path="blockbatch/contract_creation/create_traces_v1",
    enforce_row_count=True,
)

ERC20_TRANSFERS = LoadSpec.pass_through(
    root_path="blockbatch/token_transfers/erc20_transfers_v1",
    # We don't enforce row count for this dataset because the INSERT sql
    # includes a filter to include only rows where amount_lossless is not NULL.
    enforce_row_count=False,
)

ERC721_TRANSFERS = LoadSpec.pass_through(
    root_path="blockbatch/token_transfers/erc721_transfers_v1",
    enforce_row_count=True,
)
