import duckdb

from op_datasets.etl.intermediate.registry import register_model
from op_datasets.etl.intermediate.task import IntermediateModelsTask
from op_datasets.etl.intermediate.udfs import Expression, wei_to_eth, wei_to_gwei, safe_div, to_sql


# Reused expressions

BLOCK_HOUR = "datepart('hour', make_timestamp(block_timestamp::BIGINT * 1000000))"
TX_SUCCESS = "receipt_status = 1"
TX_FAIL = "receipt_status != 1"

L2_CONTRIB_GAS = "(gas_price * receipt_gas_used)"
L1_CONTRIB_GAS = "receipt_l1_fee"

TOTAL_GAS = f"{L2_CONTRIB_GAS} + {L1_CONTRIB_GAS}"

L1_BLOB_GAS_CONTRIB = (
    "receipt_l1_gas_used * receipt_l1_blob_base_fee_scalar * receipt_l1_blob_base_fee"
)
L1_L1_GAS_CONTRIB = "receipt_l1_gas_used * COALESCE(receipt_l1_base_fee_scalar, receipt_l1_fee_scalar) * receipt_l1_gas_price"

L2_CONTRIB_PRIORITY = "(max_priority_fee_per_gas * receipt_gas_used)"


AGGREGATION_EXPRS = [
    # Transactions
    Expression(
        alias="total_txs",
        sql_expr="COUNT(hash)",
    ),
    Expression(
        alias="total_txs_success",
        sql_expr=f"COUNT_IF({TX_SUCCESS})",
    ),
    Expression(
        alias="total_txs_fail",
        sql_expr=f"COUNT_IF({TX_FAIL})",
    ),
    # Blocks
    Expression(
        alias="total_blocks",
        sql_expr="COUNT(DISTINCT block_number)",
    ),
    Expression(
        alias="total_blocks_success",
        sql_expr=f"COUNT(DISTINCT IF({TX_SUCCESS}, block_number, NULL))",
    ),
    Expression(
        alias="total_blocks_fail",
        sql_expr=f"COUNT(DISTINCT IF({TX_FAIL}, block_number, NULL))",
    ),
    Expression(
        alias="min_block_number",
        sql_expr="MIN(block_number)",
    ),
    Expression(
        alias="max_block_number",
        sql_expr="MAX(block_number)",
    ),
    Expression(
        alias="block_interval_active",
        sql_expr="MAX(block_number) - MIN(block_number) + 1",
    ),
    # Nonce
    Expression(
        alias="min_nonce",
        sql_expr="MIN(nonce)",
    ),
    Expression(
        alias="max_nonce",
        sql_expr="MAX(nonce)",
    ),
    Expression(
        alias="nonce_interval_active",
        sql_expr="MAX(nonce) - MIN(nonce) + 1",
    ),
    # Gas Usage
    Expression(
        alias="total_l2_gas_used",
        sql_expr="SUM(receipt_gas_used)",
    ),
    Expression(
        alias="total_l2_gas_used_success",
        sql_expr=f"SUM(IF({TX_SUCCESS}, receipt_gas_used, 0))",
    ),
    Expression(
        alias="total_l2_gas_used_fail",
        sql_expr=f"SUM(IF({TX_FAIL}, receipt_gas_used, 0))",
    ),
    Expression(
        alias="total_l1_gas_used",
        sql_expr="SUM(receipt_l1_gas_used)",
    ),
    Expression(
        alias="total_l1_gas_used_success",
        sql_expr=f"SUM(IF({TX_SUCCESS}, receipt_l1_gas_used, 0))",
    ),
    Expression(
        alias="total_l1_gas_used_fail",
        sql_expr=f"SUM(IF({TX_FAIL}, receipt_l1_gas_used, 0))",
    ),
    # Gas Fee Paid
    Expression(
        alias="total_gas_fees",
        sql_expr=wei_to_eth(f"SUM({TOTAL_GAS})"),
    ),
    Expression(
        alias="total_gas_fees_success",
        sql_expr=wei_to_eth(f"SUM(IF({TX_SUCCESS}, {TOTAL_GAS}, 0))"),
    ),
    Expression(
        alias="total_gas_fees_fail",
        sql_expr=wei_to_eth(f"SUM(IF({TX_FAIL}, {TOTAL_GAS}, 0))"),
    ),
    # Gas Fee Breakdown
    Expression(
        alias="l2_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L2_CONTRIB_GAS})"),
    ),
    Expression(
        alias="l1_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L1_CONTRIB_GAS})"),
    ),
    Expression(
        alias="l1_blobgas_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L1_BLOB_GAS_CONTRIB})"),
    ),
    Expression(
        alias="l1_l1gas_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L1_L1_GAS_CONTRIB})"),
    ),
    Expression(
        alias="l2_contrib_gas_fees_base_fees",
        sql_expr=wei_to_eth(f"SUM({L2_CONTRIB_GAS} - {L2_CONTRIB_PRIORITY})"),
    ),
    Expression(
        alias="l2_contrib_gas_fees_priority_fees",
        sql_expr=wei_to_eth(f"SUM({L2_CONTRIB_PRIORITY})"),
    ),
    # Average Gas Fee
    Expression(
        alias="avg_l2_gas_price_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                f"SUM({L2_CONTRIB_GAS})",
                "SUM(receipt_gas_used)",
            )
        ),
    ),
    Expression(
        alias="avg_l2_base_fee_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                f"SUM({L2_CONTRIB_GAS} - {L2_CONTRIB_PRIORITY})",
                "SUM(receipt_gas_used)",
            )
        ),
    ),
    Expression(
        alias="avg_l2_priority_fee_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                f"SUM({L2_CONTRIB_PRIORITY})",
                "SUM(receipt_gas_used)",
            )
        ),
    ),
    Expression(
        alias="avg_l1_gas_price_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                "SUM(receipt_l1_gas_price * receipt_l1_gas_used)",
                "SUM(receipt_l1_gas_used)",
            )
        ),
    ),
    Expression(
        alias="avg_l1_blob_base_fee_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                "SUM(receipt_l1_blob_base_fee * receipt_l1_gas_used)",
                "SUM(receipt_l1_gas_used)",
            )
        ),
    ),
    # Block Time
    Expression(
        alias="min_block_timestamp",
        sql_expr="MIN(block_timestamp)",
    ),
    Expression(
        alias="max_block_timestamp",
        sql_expr="MAX(block_timestamp)",
    ),
    Expression(
        alias="time_interval_active",
        sql_expr="MAX(block_timestamp) - MIN(block_timestamp)",
    ),
    Expression(alias="unique_hours_active", sql_expr=f"COUNT(DISTINCT {BLOCK_HOUR})"),
    # To addresses, (todos: identify contracts in the future)
    Expression(
        alias="num_to_addresses",
        sql_expr="COUNT(DISTINCT to_address)",
    ),
    Expression(
        alias="num_to_addresses_success",
        sql_expr=f"COUNT(DISTINCT IF({TX_SUCCESS}, to_address, NULL))",
    ),
    Expression(
        alias="num_to_addresses_fail",
        sql_expr=f"COUNT(DISTINCT IF({TX_FAIL}, to_address, NULL))",
    ),
]


@register_model
def daily_address_summary(task: IntermediateModelsTask) -> dict[str, duckdb.DuckDBPyRelation]:
    query = f"""
    SELECT
        dt,
        chain,
        chain_id,
        from_address,
        {to_sql(AGGREGATION_EXPRS)}
    FROM txs
    WHERE gas_price > 0

    -- Optional address filter for faster results when developoing.
    -- AND from_address LIKE '0x00%'

    GROUP BY 1, 2, 3, 4
    """

    # Uncomment when debugging:
    # print(query)

    txs: duckdb.DuckDBPyRelation = task.input_duckdb_relations["transactions"]
    results = txs.query("txs", query)

    # Model functions always return a dictionary of output results.
    return {"daily_address_summary": results}
