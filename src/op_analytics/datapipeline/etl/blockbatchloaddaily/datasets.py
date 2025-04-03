from .loadspec import LoadSpec

TRACES_AGG1 = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtraces/daily_trfrom_trto_v1",
)


TRACES_AGG2 = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtraces/daily_trto_v1",
)


TRACES_AGG3 = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtraces/daily_trto_txto_txmethod_v1",
)

DAILY_ADDRESS_SUMMARY = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_transactions_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtxs/daily_address_summary_v1",
    ignore_non_zero_row_count=[
        "xterio",
    ],
)
