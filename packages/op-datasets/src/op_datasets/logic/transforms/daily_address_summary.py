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
    SELECT
        {groupby_clause}

        -- transactions
        ,COUNT(hash) AS total_txs
        ,COUNT(CASE WHEN receipt_status = 1 THEN hash ELSE NULL END) AS total_txs_success
        ,COUNT(CASE WHEN receipt_status != 1 THEN hash ELSE NULL END) AS total_txs_fail

        -- blocks
        ,COUNT(DISTINCT block_number) AS total_blocks
        ,COUNT(DISTINCT CASE WHEN receipt_status = 1 THEN block_number ELSE NULL END) AS total_blocks_success
        ,COUNT(DISTINCT CASE WHEN receipt_status != 1 THEN block_number ELSE NULL END) AS total_blocks_fail
        ,MIN(block_number) AS min_block_number
        ,MAX(block_number) AS max_block_number
        ,MAX(block_number) - MIN(block_number) + 1 AS block_interval_active

        -- nonce
        ,MIN(nonce) AS min_nonce
        ,MAX(nonce) AS max_nonce
        ,MAX(nonce) - MIN(nonce) + 1 AS nonce_interval_active

        -- gas usage
        ,SUM(receipt_gas_used) AS total_l2_gas_used
        ,SUM(CASE WHEN receipt_status = 1 THEN receipt_gas_used ELSE 0 END) AS total_l2_gas_used_success
        ,SUM(CASE WHEN receipt_status != 1 THEN receipt_gas_used ELSE 0 END) AS total_l2_gas_used_fail
        ,SUM(receipt_l1_gas_used) AS total_l1_gas_used
        ,SUM(CASE WHEN receipt_status = 1 THEN receipt_l1_gas_used ELSE 0 END) AS total_l1_gas_used_success
        ,SUM(CASE WHEN receipt_status != 1 THEN receipt_l1_gas_used ELSE 0 END) AS total_l1_gas_used_fail

        -- gas fee paid
        ,SUM (
         CAST ( ( (gas_price * receipt_gas_used) + receipt_l1_fee ) AS DECIMAL(38,18) )
         / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
        ) AS total_gas_fees
        ,SUM (CASE WHEN receipt_status = 1 THEN
         CAST ( ( (gas_price * receipt_gas_used) + receipt_l1_fee ) AS DECIMAL(38,18) )
         / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
        ELSE 0 END) AS total_gas_fees_success
        ,SUM (CASE WHEN receipt_status != 1 THEN
         CAST ( ( (gas_price * receipt_gas_used) + receipt_l1_fee ) AS DECIMAL(38,18) )
         / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
        ELSE 0 END) AS total_gas_fees_fail

        -- fee breakdown
        ,SUM( CAST( (gas_price * receipt_gas_used) AS DECIMAL(38,18) ) / CAST( 1000000000000000000 AS DECIMAL(18, 0)) ) AS l2_contrib_gas_fees
        ,SUM( CAST(receipt_l1_fee AS DECIMAL(38,18) ) / CAST( 1000000000000000000 AS DECIMAL(18, 0)) ) AS l1_contrib_gas_fees

        ,SUM( CAST( (receipt_l1_gas_used * receipt_l1_blob_base_fee_scalar * receipt_l1_blob_base_fee) AS DECIMAL(38,18) )
            / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
            ) AS l1_blobgas_contrib_gas_fees
        ,SUM( CAST( (receipt_l1_gas_used * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar) * receipt_l1_gas_price) AS DECIMAL(38,18) )
            / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
            ) AS l1_l1gas_contrib_gas_fees

        ,SUM(
            CAST( (gas_price - max_priority_fee_per_gas) * receipt_gas_used AS DECIMAL(38,18) )
            / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
        ) AS l2_contrib_gas_fees_base_fee
        ,SUM(
            CAST( max_priority_fee_per_gas * receipt_gas_used AS DECIMAL(38,18) )
            / CAST( 1000000000000000000 AS DECIMAL(18, 0) )
        ) AS l2_contrib_gas_fees_priority_fee

        -- average fees
        ,SUM( CAST( (gas_price * receipt_gas_used) AS DECIMAL(38,9) ) / CAST( 1000000000 AS DECIMAL(9, 0)) ) / SUM( CAST(receipt_gas_used AS DECIMAL(38,9)) ) AS avg_l2_gas_price_gwei
        ,SUM( CAST( ((gas_price - max_priority_fee_per_gas) * receipt_gas_used) AS DECIMAL(38,9) ) / CAST( 1000000000 AS DECIMAL(9, 0)) ) / SUM( CAST(receipt_gas_used AS DECIMAL(38,9)) ) AS avg_l2_base_fee_gwei
        ,SUM( CAST( (max_priority_fee_per_gas * receipt_gas_used) AS DECIMAL(38,9) ) / CAST( 1000000000 AS DECIMAL(9, 0)) ) / SUM( CAST(receipt_gas_used AS DECIMAL(38,9)) ) AS avg_l2_priority_fee_gwei
        ,SUM( CAST( (receipt_l1_gas_price * receipt_l1_gas_used) AS DECIMAL(38,9) ) / CAST( 1000000000 AS DECIMAL(9, 0)) ) / SUM( CAST(receipt_l1_gas_used AS DECIMAL(38,9)) ) AS avg_l1_gas_price_gwei
        ,SUM( CAST( (receipt_l1_blob_base_fee * receipt_l1_gas_used) AS DECIMAL(38,9) ) / CAST( 1000000000 AS DECIMAL(9, 0)) ) / SUM( CAST(receipt_l1_gas_used AS DECIMAL(38,9)) ) AS avg_l1_blob_base_fee_gwei

        -- block time
        ,MIN(block_timestamp) AS min_block_timestamp
        ,MAX(block_timestamp) AS max_block_timestamp
        ,MAX(block_timestamp) - MIN(block_timestamp) AS time_interval_active
        ,COUNT(DISTINCT hour) AS unique_hours_active
        ,CAST(COUNT(DISTINCT hour) AS FLOAT) / 24 AS perc_daily_hours_active

        -- to addresses, to identify contracts in the future
        ,COUNT(DISTINCT to_address) AS num_to_addresses
        ,COUNT(DISTINCT CASE WHEN receipt_status = 1 THEN to_address ELSE NULL END) AS num_to_addresses_success
        ,COUNT(DISTINCT CASE WHEN receipt_status != 1 THEN to_address ELSE NULL END) AS num_to_addresses_fail

    FROM
        _filter_df
    GROUP BY
        {groupby_clause}
    """

    return pl.sql(query).collect()
