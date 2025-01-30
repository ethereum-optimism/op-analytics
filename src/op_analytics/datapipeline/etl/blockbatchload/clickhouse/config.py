# Root paths that we want to ingest from GCS into clickhouse.
# Add more root paths here as we ingest more datasets.
LOADABLE_ROOT_PATHS = [
    "blockbatch/contract_creation/create_traces_v1",
    "blockbatch/token_transfers/erc20_transfers_v1",
    "blockbatch/token_transfers/erc721_transfers_v1",
]


# Root paths for which we allow filtering as part of loading.
FILTER_ALLOWED_ROOT_PATHS = [
    "blockbatch/token_transfers/erc20_transfers_v1",
]
