# -*- coding: utf-8 -*-
import polars as pl


def daily_address_summary(
    df: pl.DataFrame, conditions: dict[str, callable], **groupby_args: dict[str, str]
) -> pl.DataFrame:
    # Create filter expression from conditions dictionary
    filter_expr = pl.lit(True)
    for col, condition in conditions.items():
        filter_expr &= condition(pl.col(col))

    # Apply the filter to the DataFrame
    _filter_df = df.filter(filter_expr)

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
        ,SUM(receipt_gas_used) AS total_gas_used
        ,SUM(CASE WHEN receipt_status = 1 THEN receipt_gas_used ELSE 0 END) AS total_gas_used_success
        ,SUM(CASE WHEN receipt_status != 1 THEN receipt_gas_used ELSE 0 END) AS total_gas_used_fail

        -- block timestamp
        ,MIN(block_timestamp) AS min_block_timestamp
        ,MAX(block_timestamp) AS max_block_timestamp
        ,MAX(block_timestamp) - MIN(block_timestamp) AS time_interval_active

        -- to addresses, to identify contracts in the future
        ,COUNT(DISTINCT to_address) AS num_to_addresses
        ,COUNT(DISTINCT CASE WHEN receipt_status = 1 THEN to_address ELSE NULL END) AS num_to_addresses_success
        ,COUNT(DISTINCT CASE WHEN receipt_status != 1 THEN to_address ELSE NULL END) AS num_to_addresses_fail

        -- get number of hours active

    FROM
        _filter_df
    GROUP BY
        {groupby_clause}
    """

    # Execute the query and collect the result
    result = pl.sql(query).collect()

    return result
