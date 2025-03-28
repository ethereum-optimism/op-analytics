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

    AGG1 = "daily_trfrom_trto_v1"
    AGG2 = "daily_trto_v1"
    AGG3 = "daily_trto_txto_txmethod_v1"

    for agg in [AGG1, AGG2, AGG3]:
        result = daily_to_clickhouse(
            dataset=LoadSpec(
                input_root_paths=[
                    "blockbatch/refined_traces/refined_traces_fees_v2",
                ],
                output_root_path=f"blockbatch_daily/aggtraces/{agg}",
                enforce_row_count=False,
            ),
            range_spec="m4days",
            num_workers=2,
        )
        context.log.info(result)
