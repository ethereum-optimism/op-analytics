from .loadspec_date import ClickHouseDateETL
from .loadspec_datechain import ClickHouseDateChainETL

ALLOWED_EMPTY_CHAINS = [
    "arenaz",
    "kroma",
    "race",
    "xterio",
]

ALLOWED_EMPTY_DATES = [
    # Dates near soneium activation date.
    ("soneium", "2024-12-02"),
    ("soneium", "2024-12-05"),
    ("soneium", "2024-12-07"),
    #
    # Dates near ink activation date.
    ("ink", "2024-12-06"),
    ("ink", "2024-12-07"),
    ("ink", "2024-12-08"),
    #
    ("unichain", "2024-11-09"),
    #
    ("swell", "2024-11-27"),
    ("swell", "2024-11-30"),
]

TRACES_AGG1 = ClickHouseDateChainETL(
    output_root_path="blockbatch_daily/aggtraces/daily_trfrom_trto_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)


TRACES_AGG2 = ClickHouseDateChainETL(
    output_root_path="blockbatch_daily/aggtraces/daily_trto_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)


TRACES_AGG3 = ClickHouseDateChainETL(
    output_root_path="blockbatch_daily/aggtraces/daily_trto_txto_txmethod_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)


DAILY_ADDRESS_SUMMARY = ClickHouseDateChainETL(
    output_root_path="blockbatch_daily/aggtxs/daily_address_summary_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_transactions_fees_v2",
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)

DAILY_SEGMENTS = ClickHouseDateChainETL(
    output_root_path="blockbatch_daily/aggtxs/daily_segments_v1",
    inputs_clickhouse=[
        DAILY_ADDRESS_SUMMARY.output_root_path,
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)

DAILY_TX_ACTIVITY_SEGMENTS = ClickHouseDateChainETL(
    output_root_path="blockbatch_daily/wallet_segments/daily_tx_activity_segments_v1",
    inputs_clickhouse=[
        DAILY_ADDRESS_SUMMARY.output_root_path,
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)


# This dataset is not used in production. It was created for testing purposes.
# This is an aggregation across all chains at the date level.
DUMMY_AGGREGATE = ClickHouseDateETL(
    output_root_path="transforms_dummy/daily_counts_v0",
    inputs_clickhouse=["blockbatch_daily/aggtxs/daily_address_summary_v1"],
)
