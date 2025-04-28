from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.datapipeline.etl.blockbatchloaddaily.main import (
    daily_to_clickhouse,
)

from op_analytics.datapipeline.etl.blockbatchloaddaily.datasets import (
    TRACES_AGG1,
    TRACES_AGG2,
    TRACES_AGG3,
    DAILY_ADDRESS_SUMMARY,
)

# NOTE: It is important to schedule all of the assets below in the same dagster job.
# This will ensure that they run in series, which is preferred so that we don't
# overload the ClickHouse database.


@asset
def aggreagated_traces(context: OpExecutionContext):
    """Load aggregated traces blockbatch data to Clickhouse."""

    for agg in [TRACES_AGG1, TRACES_AGG2, TRACES_AGG3, DAILY_ADDRESS_SUMMARY]:
        result = daily_to_clickhouse(
            dataset=agg,
            range_spec="m4days",
            num_workers=2,
            dagster_context=context,
        )
        context.log.info(result)
