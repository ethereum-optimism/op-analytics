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

JOIN_COLUMNS = ["chain", "protocol_slug", "token"]


def calculate_net_flows(
    df: pl.DataFrame,
    lookback_df: pl.DataFrame,
    flow_days: list,
) -> pl.DataFrame:
    """Add net_token_flow_Xd colums for every X in the flow_days list.

    The data for the current date is in "df" and "lookback_df" is expected to have data
    for each of the lookback days in flow_days.
    """

    # Compute the USD conversion rate for each row.
    # If there is 0 TVL we coerce the conversion rate to 0 from +/-inf.
    result = df.with_columns(
        [
            (pl.col("app_token_tvl_usd") / pl.col("app_token_tvl"))
            .replace([float("inf"), float("-inf")], [0, 0])
            .alias("usd_conversion_rate")
        ]
    )

    assert result.columns == TVL_COLUMNS

    # Keep track of the net flow columns that have been added to the result dataframe.
    added_tvl_columns: list[str] = []
    added_flow_columns: list[str] = []

    # Loop over each of the flow days and add the net_flow column for that lookback.
    for d in flow_days:
        filtered = lookback_df.filter(pl.col("lookback") == d)

        try:
            right = filtered.drop("dt", "lookback")
            result = result.join(right, on=JOIN_COLUMNS, how="left", validate="1:1")
        except Exception as ex:
            dtval = filtered["dt"].unique()
            raise Exception(f"duplicate rows found on dt={dtval}") from ex

        # Joining adds the token TVL value at the lookback date.
        assert result.columns[-1] == "app_token_tvl_right"

        # Compute the net token flow.
        lookback_tvl_col = f"app_token_tvl_{d}d"
        net_flow_col = f"net_token_flow_{d}d"

        result = result.with_columns(
            [
                # Token TVL at the lookback date.
                pl.col("app_token_tvl_right").alias(lookback_tvl_col),
                #
                # Net TVL Flow from the lookback date.
                (
                    pl.col("app_token_tvl_usd")
                    - (pl.col("app_token_tvl_right") * pl.col("usd_conversion_rate"))
                )
                .fill_nan(0)
                .alias(net_flow_col),
            ]
        ).drop("app_token_tvl_right")

        # Computing adds the net flow column.
        added_tvl_columns.append(lookback_tvl_col)
        added_flow_columns.append(net_flow_col)

        # Zip the added columns to get what should be the curent schema.
        flattened_columns = [
            col for pair in zip(added_tvl_columns, added_flow_columns) for col in pair
        ]
        assert result.columns == TVL_COLUMNS + flattened_columns

    log.info(f"computed net flow for lookback={flow_days} days")

    # Reorder the columns so that there is a group of TVL looback columns
    # and then a group of net flow columns.
    return result.select(TVL_COLUMNS + added_tvl_columns + added_flow_columns)
