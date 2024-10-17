# -*- coding: utf-8 -*-
import polars as pl

CONDITIONS = {
    "gas_price": lambda x: x > 0,
}


def daily_address_summary(
    df: pl.DataFrame, conditions: dict[str, callable], **groupby_args: dict[str, str]
) -> pl.DataFrame:
    """Create a daily summary addresses and activities such as transaction count, gas usage, fees and more across the Superchain.

    The main dataset we pull stats from is the `transactions` dataset.
    We can filter the data based on the conditions set in the CONDITIONS dictionary.
    The groupby_args dictionary is used to group the data by the columns specified in the dictionary.

    In our pipeline, by default the function will group the following columns:
    - `from_address`: the address that initiated a transaction
    - `chain_id`: the chain id that the address
    - `chain`: the chain name of a transaction
    - `dt`: date granularity in the string format `YYYY-MM-DD`
    """
    # Create filter expression from conditions dictionary
    filter_expr = pl.lit(True)
    for col, condition in conditions.items():
        filter_expr &= condition(pl.col(col))

    # Apply the filter to the DataFrame
    _filter_df = df.filter(filter_expr)

    # convert timestamp from epoch to datetime
    _filter_df = _filter_df.with_columns(
        [
            pl.from_epoch(pl.col("block_timestamp")),
            pl.from_epoch(pl.col("block_timestamp"), time_unit="s")
            .dt.hour()
            .alias("hour"),
        ]
    )
    # Extract groupby columns from the arguments
    groupby_cols = list(groupby_args.values())

    # SQL query to perform aggregations
    groupby_clause = ", ".join(groupby_cols)

    query = f"""
    WITH constants AS (
        SELECT
            CAST(1000000000000000000 AS DECIMAL(18, 0)) AS WEI_IN_ETH,
            CAST(1000000000 AS DECIMAL(9, 0)) AS WEI_IN_GWEI
    )
    ,df_enrich AS (
        SELECT
            _filter_df.*

            -- Gas Fee Breakdown
            ,CAST(((gas_price * receipt_gas_used) + receipt_l1_fee) AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS total_gas_fee
            ,CASE
                WHEN receipt_status = 1 THEN CAST(((gas_price * receipt_gas_used) + receipt_l1_fee) AS DECIMAL(38,18)) / constants.WEI_IN_ETH
                ELSE 0
                END AS total_gas_fee_success
            ,CASE
                WHEN receipt_status != 1 THEN CAST(((gas_price * receipt_gas_used) + receipt_l1_fee) AS DECIMAL(38,18)) / constants.WEI_IN_ETH
                ELSE 0
                END AS total_gas_fee_fail
            ,CAST((gas_price * receipt_gas_used) AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS l2_contrib_gas_fee
            ,CAST(receipt_l1_fee AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS l1_contrib_gas_fee
            ,CAST((receipt_l1_gas_used * receipt_l1_blob_base_fee_scalar * receipt_l1_blob_base_fee) AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS l1_blobgas_contrib_gas_fee
            ,CAST((receipt_l1_gas_used * COALESCE(receipt_l1_base_fee_scalar, receipt_l1_fee_scalar) * receipt_l1_gas_price) AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS l1_l1gas_contrib_gas_fee
            ,CAST((gas_price - max_priority_fee_per_gas) * receipt_gas_used AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS l2_contrib_gas_fee_base_fee
            ,CAST(max_priority_fee_per_gas * receipt_gas_used AS DECIMAL(38,18)) / constants.WEI_IN_ETH AS l2_contrib_gas_fee_priority_fee

            -- Gas Price Breakdown
            ,CAST((gas_price * receipt_gas_used) AS DECIMAL(38,9)) / constants.WEI_IN_GWEI AS total_gas_price_gwei
            ,CAST(((gas_price - max_priority_fee_per_gas) * receipt_gas_used) AS DECIMAL(38,9)) / constants.WEI_IN_GWEI AS base_fee_gwei
            ,CAST(max_priority_fee_per_gas * receipt_gas_used AS DECIMAL(38,9)) / constants.WEI_IN_GWEI AS priority_fee_gwei
            ,CAST(receipt_l1_gas_price * receipt_l1_gas_used AS DECIMAL(38,9)) / constants.WEI_IN_GWEI AS l1_gas_price_gwei
            ,CAST(receipt_l1_blob_base_fee * receipt_l1_gas_used AS DECIMAL(38,9)) / constants.WEI_IN_GWEI AS l1_blob_base_fee_gwe
        FROM
            _filter_df
        CROSS JOIN constants
    )

    SELECT
        {groupby_clause}

        -- Transactions
        ,COUNT(hash) AS total_txs
        ,COUNT(CASE WHEN receipt_status = 1 THEN hash ELSE NULL END) AS total_txs_success
        ,COUNT(CASE WHEN receipt_status != 1 THEN hash ELSE NULL END) AS total_txs_fail

        -- Blocks
        ,COUNT(DISTINCT block_number) AS total_blocks
        ,COUNT(DISTINCT CASE WHEN receipt_status = 1 THEN block_number ELSE NULL END) AS total_blocks_success
        ,COUNT(DISTINCT CASE WHEN receipt_status != 1 THEN block_number ELSE NULL END) AS total_blocks_fail
        ,MIN(block_number) AS min_block_number
        ,MAX(block_number) AS max_block_number
        ,MAX(block_number) - MIN(block_number) + 1 AS block_interval_active

        -- Nonce
        ,MIN(nonce) AS min_nonce
        ,MAX(nonce) AS max_nonce
        ,MAX(nonce) - MIN(nonce) + 1 AS nonce_interval_active

        -- Gas Usage
        ,SUM(receipt_gas_used) AS total_l2_gas_used
        ,SUM(CASE WHEN receipt_status = 1 THEN receipt_gas_used ELSE 0 END) AS total_l2_gas_used_success
        ,SUM(CASE WHEN receipt_status != 1 THEN receipt_gas_used ELSE 0 END) AS total_l2_gas_used_fail
        ,SUM(receipt_l1_gas_used) AS total_l1_gas_used
        ,SUM(CASE WHEN receipt_status = 1 THEN receipt_l1_gas_used ELSE 0 END) AS total_l1_gas_used_success
        ,SUM(CASE WHEN receipt_status != 1 THEN receipt_l1_gas_used ELSE 0 END) AS total_l1_gas_used_fail

        -- Gas Fee Paid
        ,SUM(total_gas_fee) AS total_gas_fees
        ,SUM(total_gas_fee_success) AS total_gas_fees_success
        ,SUM(total_gas_fee_fail) AS total_gas_fees_fail

        -- Gas Fee Breakdown
        ,SUM(l2_contrib_gas_fee) AS l2_contrib_gas_fees
        ,SUM(l1_contrib_gas_fee) AS l1_contrib_gas_fees
        ,SUM(l1_blobgas_contrib_gas_fee) AS l1_blobgas_contrib_gas_fees
        ,SUM(l1_l1gas_contrib_gas_fee) AS l1_l1gas_contrib_gas_fees
        ,SUM(l2_contrib_gas_fee_base_fee) AS l2_contrib_gas_fees_base_fee
        ,SUM(l2_contrib_gas_fee_priority_fee) AS l2_contrib_gas_fees_priority_fee

        -- Average Gas Fee
        ,SUM(total_gas_price_gwei) / SUM(CAST(receipt_gas_used AS DECIMAL(38,9))) AS avg_l2_gas_price_gwei
        ,SUM(base_fee_gwei) / SUM(CAST(receipt_gas_used AS DECIMAL(38,9))) AS avg_l2_base_fee_gwei
        ,SUM(priority_fee_gwei) / SUM(CAST(receipt_gas_used AS DECIMAL(38,9))) AS avg_l2_priority_fee_gwei
        ,SUM(l1_gas_price_gwei) / SUM(CAST(receipt_l1_gas_used AS DECIMAL(38,9))) AS avg_l1_gas_price_gwei
        ,SUM(l1_blob_base_fee_gwei) / SUM(CAST(receipt_l1_gas_used AS DECIMAL(38,9))) AS avg_l1_blob_base_fee_gwei

        -- Block Time
        ,MIN(block_timestamp) AS min_block_timestamp
        ,MAX(block_timestamp) AS max_block_timestamp
        ,MAX(block_timestamp) - MIN(block_timestamp) AS time_interval_active
        ,COUNT(DISTINCT hour) AS unique_hours_active
        ,CAST(COUNT(DISTINCT hour) AS FLOAT) / 24 AS perc_daily_hours_active

        -- To addresses, (todos: identify contracts in the future)
        ,COUNT(DISTINCT to_address) AS num_to_addresses
        ,COUNT(DISTINCT CASE WHEN receipt_status = 1 THEN to_address ELSE NULL END) AS num_to_addresses_success
        ,COUNT(DISTINCT CASE WHEN receipt_status != 1 THEN to_address ELSE NULL END) AS num_to_addresses_fail

    FROM
        df_enrich
    GROUP BY
        {groupby_clause}
    """

    result = pl.sql(query).collect()

    actual_schema = result.collect_schema()

    groupby_col_names = actual_schema.names()[: len(groupby_cols)]
    groupby_col__dtypes = actual_schema.dtypes()[: len(groupby_cols)]

    dynamic_schema = dict(zip(groupby_col_names, groupby_col__dtypes))

    base_schema = {
        "total_txs": pl.UInt32,
        "total_txs_success": pl.UInt32,
        "total_txs_fail": pl.UInt32,
        "total_blocks": pl.UInt32,
        "total_blocks_success": pl.UInt32,
        "total_blocks_fail": pl.UInt32,
        "min_block_number": pl.Int64,
        "max_block_number": pl.Int64,
        "block_interval_active": pl.Int64,
        "min_nonce": pl.Int64,
        "max_nonce": pl.Int64,
        "nonce_interval_active": pl.Int64,
        "total_l2_gas_used": pl.Int64,
        "total_l2_gas_used_success": pl.Int64,
        "total_l2_gas_used_fail": pl.Int64,
        "total_l1_gas_used": pl.Int64,
        "total_l1_gas_used_success": pl.Int64,
        "total_l1_gas_used_fail": pl.Int64,
        "total_gas_fees": pl.Decimal,
        "total_gas_fees_success": pl.Decimal,
        "total_gas_fees_fail": pl.Decimal,
        "l2_contrib_gas_fees": pl.Decimal,
        "l1_contrib_gas_fees": pl.Decimal,
        "l1_blobgas_contrib_gas_fees": pl.Decimal,
        "l1_l1gas_contrib_gas_fees": pl.Decimal,
        "l2_contrib_gas_fees_base_fee": pl.Decimal,
        "l2_contrib_gas_fees_priority_fee": pl.Decimal,
        "avg_l2_gas_price_gwei": pl.Decimal,
        "avg_l2_base_fee_gwei": pl.Decimal,
        "avg_l2_priority_fee_gwei": pl.Decimal,
        "avg_l1_gas_price_gwei": pl.Decimal,
        "avg_l1_blob_base_fee_gwei": pl.Decimal,
        "min_block_timestamp": pl.Datetime,
        "max_block_timestamp": pl.Datetime,
        "time_interval_active": pl.Duration,
        "unique_hours_active": pl.UInt32,
        "perc_daily_hours_active": pl.Float64,
        "num_to_addresses": pl.UInt32,
        "num_to_addresses_success": pl.UInt32,
        "num_to_addresses_fail": pl.UInt32,
    }

    full_expected_schema = pl.Schema({**dynamic_schema, **base_schema})

    assert actual_schema == full_expected_schema

    return result
