from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.datapipeline.etl.blockbatchloaddaily.main import (
    LoadSpec,
    daily_to_clickhouse,
)

# NOTE: It is important to schedule all of the assets below in the same dagster job.
# This will ensure that they run in series, which is preferred so that we don't
# overload the ClickHouse database.


@asset
def aggreagated_traces(context: OpExecutionContext):
    """Load aggregated traces blockbatch data to Clickhouse."""

    result = daily_to_clickhouse(
        LoadSpec(
            input_root_paths=[
                "blockbatch/refined_traces/refined_transactions_fees_v1",
                "blockbatch/refined_traces/refined_traces_fees_v1",
            ],
            output_root_path="blockbatch/aggregated_traces/traces_trgrain_v3",
            enforce_row_count=False,
        ),
    )
    context.log.info(result)

    result = daily_to_clickhouse(
        LoadSpec(
            input_root_paths=[
                "blockbatch/refined_traces/refined_transactions_fees_v1",
                "blockbatch/refined_traces/refined_traces_fees_v1",
            ],
            output_root_path="blockbatch/aggregated_traces/traces_txgrain_v3",
            enforce_row_count=False,
        ),
    )
    context.log.info(result)
