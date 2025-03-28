from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.datapipeline.etl.blockbatchload.main import (
    LoadSpec,
    load_to_clickhouse,
)

# NOTE: It is important to schedule all of the assets below in the same dagster job.
# This will ensure that they run in series, which is preferred so that we don't
# overload the ClickHouse database.


@asset
def contract_creation(context: OpExecutionContext):
    """Load contract creation blockbatch data to Clickhouse."""
    result = load_to_clickhouse(
        datasets=[
            LoadSpec.pass_through(
                root_path="blockbatch/contract_creation/create_traces_v1",
                enforce_row_count=True,
            ),
        ]
    )
    context.log.info(result)


@asset
def erc20_transfers(context: OpExecutionContext):
    """Load ERC-20 transfers blockbatch data to Clickhouse."""
    result = load_to_clickhouse(
        datasets=[
            LoadSpec.pass_through(
                root_path="blockbatch/token_transfers/erc20_transfers_v1",
                # We don't enforce row count for this dataset because the INSERT sql
                # includes a filter to include only rows where amount_lossless is not NULL.
                enforce_row_count=False,
            ),
        ]
    )
    context.log.info(result)


@asset
def erc721_transfers(context: OpExecutionContext):
    """Load ERC-721 transfers blockbatch data to Clickhouse."""
    result = load_to_clickhouse(
        datasets=[
            LoadSpec.pass_through(
                root_path="blockbatch/token_transfers/erc721_transfers_v1",
                enforce_row_count=True,
            ),
        ]
    )
    context.log.info(result)
