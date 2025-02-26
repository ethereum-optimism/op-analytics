import polars as pl

from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


# Columns present in the TVL input data.
TVL_COLUMNS = [
    "dt",
    "chain",
    "protocol_slug",
    "token",
    "app_token_tvl",
    "app_token_tvl_usd",
    "usd_conversion_rate",
]


def calculate_net_flows(
    df: pl.DataFrame,
    lookback_df: pl.DataFrame,
    flow_days: list,
) -> pl.DataFrame:
    """Add net_token_flow_Xd colums for every X in the flow_days list.

    The data for the current date is in "df" and "lookback_df" is expected to have data
    for each of the lookback days in flow_days.
    """

    result = df
    assert result.columns == TVL_COLUMNS

    # Keep track of the net flow columns that have been added to the result dataframe.
    added_columns: list[str] = []

    # Loop over each of the flow days and add the net_flow column for that lookback.
    for d in flow_days:
        filtered = lookback_df.filter(pl.col("lookback") == d).drop("lookback")

        result = result.join(
            filtered,
            on=["chain", "protocol_slug", "token"],
            how="left",
            validate="1:1",
        )

        # Joining adds the token TVL value at the lookback date.
        assert result.columns == TVL_COLUMNS + added_columns + ["app_token_tvl_right"]

        # Compute the net token flow.
        net_flow_col = f"net_token_flow_{d}d"
        result = result.with_columns(
            [
                (
                    pl.col("app_token_tvl_usd")
                    - (pl.col("app_token_tvl_right") * pl.col("usd_conversion_rate"))
                )
                .fill_null(0)
                .fill_nan(0)
                .alias(net_flow_col)
            ]
        ).drop("app_token_tvl_right")

        # Computing adds the net flow column.
        added_columns.append(net_flow_col)
        assert result.columns == TVL_COLUMNS + added_columns
        log.info(f"computed net flow for lookback={d} days")

    return result
