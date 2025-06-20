from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.datapipeline.etl.blockbatchload.main import (
    load_to_clickhouse,
)

from op_analytics.datapipeline.etl.blockbatchload.datasets import (
    CONTRACT_CREATION,
    ERC20_TRANSFERS,
    ERC721_TRANSFERS,
    NATIVE_TRANSFERS,
)

# NOTE: It is important to schedule all of the assets below in the same dagster job.
# This will ensure that they run in series, which is preferred so that we don't
# overload the ClickHouse database.


@asset
def contract_creation(context: OpExecutionContext):
    """Load contract creation blockbatch data to Clickhouse."""
    result = load_to_clickhouse(dataset=CONTRACT_CREATION)
    context.log.info(result)


@asset
def erc20_transfers(context: OpExecutionContext):
    """Load ERC-20 transfers blockbatch data to Clickhouse."""
    result = load_to_clickhouse(dataset=ERC20_TRANSFERS)
    context.log.info(result)


@asset
def erc721_transfers(context: OpExecutionContext):
    """Load ERC-721 transfers blockbatch data to Clickhouse."""
    result = load_to_clickhouse(dataset=ERC721_TRANSFERS)
    context.log.info(result)


@asset
def native_transfers(context: OpExecutionContext):
    """Load native transfers blockbatch data to Clickhouse."""
    result = load_to_clickhouse(dataset=NATIVE_TRANSFERS)
    context.log.info(result)
