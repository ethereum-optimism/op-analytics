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
    DAILY_TX_ACTIVITY_SEGMENTS,
    DAILY_CROSS_CHAIN_SEGMENTS,
)

from typing import Union
from op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date import ClickHouseDateETL
from op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_datechain import (
    ClickHouseDateChainETL,
)

DatasetType = Union[ClickHouseDateChainETL, ClickHouseDateETL]

# NOTE: It is important to schedule all of the assets below in the same dagster job.
# This will ensure that they run in series, which is preferred so that we don't
# overload the ClickHouse database.


@asset
def aggreagated_traces(context: OpExecutionContext):
    """Load aggregated traces blockbatch data to Clickhouse."""

    datasets: list[DatasetType] = [
        TRACES_AGG1,
        TRACES_AGG2,
        TRACES_AGG3,
        DAILY_ADDRESS_SUMMARY,
        DAILY_TX_ACTIVITY_SEGMENTS,
        DAILY_CROSS_CHAIN_SEGMENTS,
    ]

    for agg in datasets:
        result = daily_to_clickhouse(
            dataset=agg,
            range_spec="m7days",
            num_workers=2,
            dagster_context=context,
        )
        context.log.info(result)
