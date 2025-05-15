from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def dextrades(context: OpExecutionContext):
    """Pull dex trades from Dune."""
    from op_analytics.datasources.dune.dextrades import execute_pull

    result = execute_pull()
    context.log.info(result)

@asset
def eth_price_vol(context: OpExecutionContext):
    """Pull ETH Volatility from Dune."""
    from op_analytics.datasources.dune.eth_price_vol import execute_pull

    result = execute_pull()
    context.log.info(result)

@asset
def unichain_lm(context: OpExecutionContext):
    """Pull unichain lm from Dune (Q225)."""
    from op_analytics.datasources.dune.unichain_lm import execute_pull

    result = execute_pull()
    context.log.info(result)

