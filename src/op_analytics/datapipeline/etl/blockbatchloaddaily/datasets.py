from ..blockbatchloadspec.loadspec import LoadSpec

TRACES_AGG1 = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtraces/daily_trfrom_trto_v1",
    enforce_row_count=False,
)


TRACES_AGG2 = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtraces/daily_trto_v1",
    enforce_row_count=False,
)


TRACES_AGG3 = LoadSpec(
    input_root_paths=[
        "blockbatch/refined_traces/refined_traces_fees_v2",
    ],
    output_root_path="blockbatch_daily/aggtraces/daily_trto_txto_txmethod_v1",
    enforce_row_count=False,
)
