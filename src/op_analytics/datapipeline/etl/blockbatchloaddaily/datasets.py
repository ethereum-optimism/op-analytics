from .loadspec import ClickHouseDailyDataset

TRACES_AGG1 = ClickHouseDailyDataset(
    output_root_path="blockbatch_daily/aggtraces/daily_trfrom_trto_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
)


TRACES_AGG2 = ClickHouseDailyDataset(
    output_root_path="blockbatch_daily/aggtraces/daily_trto_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
)


TRACES_AGG3 = ClickHouseDailyDataset(
    output_root_path="blockbatch_daily/aggtraces/daily_trto_txto_txmethod_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
)


ALLOWED_EMPTY_CHAINS = [
    "arenaz",
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

DAILY_ADDRESS_SUMMARY = ClickHouseDailyDataset(
    output_root_path="blockbatch_daily/aggtxs/daily_address_summary_v1",
    inputs_blockbatch=[
        "blockbatch/refined_traces/refined_transactions_fees_v2",
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)

DAILY_SEGMENTS = ClickHouseDailyDataset(
    output_root_path="blockbatch_daily/aggtxs/daily_segments_v1",
    inputs_clickhouse=[
        DAILY_ADDRESS_SUMMARY.output_root_path,
    ],
    ignore_zero_rows_chains=ALLOWED_EMPTY_CHAINS,
    ignore_zero_rows_chain_dts=ALLOWED_EMPTY_DATES,
)

DAILY_SEGMENTS_FROM_ADDRESS_SUMMARY = LoadSpec(
    input_root_paths=[
        "blockbatch_daily/aggtxs/daily_address_summary_v1",
    ],
    output_root_path="blockbatch_daily/segments/agg_daily_segments_from_address_summary_v1",
)
