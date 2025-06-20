from dagster import (
    OpExecutionContext,
    asset,
)


@asset
def daily_prices(context: OpExecutionContext):
    """Pull daily price data from CoinGecko."""
    from op_analytics.coreutils.path import repo_path
    from op_analytics.datasources.coingecko.execute import execute_pull

    # Use the same parameters as the existing job
    extra_token_ids_file = repo_path(
        "src/op_analytics/datasources/coingecko/config/extra_token_ids.txt"
    )
    result = execute_pull(
        days=365,
        extra_token_ids_file=extra_token_ids_file,
        include_top_tokens=100,
        fetch_metadata=True,
    )
    context.log.info(result)
    return result
