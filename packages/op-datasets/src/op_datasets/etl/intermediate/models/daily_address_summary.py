# -*- coding: utf-8 -*-
import duckdb
from op_coreutils.duckdb_inmem import init_client

from op_datasets.etl.intermediate.registry import register_model
from op_datasets.etl.intermediate.task import IntermediateModelsTask
from op_datasets.etl.intermediate.udfs import (
    Expression,
    wei_to_eth,
    wei_to_gwei,
    safe_div,
    to_sql,
)


# Reused expressions

BLOCK_HOUR = "datepart('hour', make_timestamp(block_timestamp::BIGINT * 1000000))"
TX_SUCCESS = "receipt_status = 1"

L2_CONTRIB_GAS_FEES = "(gas_price * receipt_gas_used)"
L1_CONTRIB_GAS_FEES = "receipt_l1_fee"

TOTAL_GAS_FEES = f"{L2_CONTRIB_GAS_FEES} + {L1_CONTRIB_GAS_FEES}"

L2_CONTRIB_PRIORITY = "(max_priority_fee_per_gas * receipt_gas_used)"
L2_CONTRIB_BASE = "(base_fee_per_gas * receipt_gas_used)"

ESTIMATED_SIZE = "receipt_l1_fee /(16*COALESCE(receipt_l1_fee_scalar,receipt_l1_base_fee_scalar)*receipt_l1_gas_price/1000000 + COALESCE( receipt_l1_blob_base_fee_scalar*receipt_l1_blob_base_fee/1000000 , 0))"

L1_CONTRIB_BLOB = f"({ESTIMATED_SIZE}) * receipt_l1_blob_base_fee_scalar/1000000 * receipt_l1_blob_base_fee"
L1_CONTRIB_L1_GAS = f"({ESTIMATED_SIZE}) * COALESCE(16*receipt_l1_base_fee_scalar/1000000, receipt_l1_fee_scalar) * receipt_l1_gas_price"


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
        alias="total_l1_gas_used",
        sql_expr=f"SUM(COALESCE(receipt_l1_gas_used, ({ESTIMATED_SIZE})))",
    ),
    Expression(
        alias="total_l1_gas_used_success",
        sql_expr=f"SUM(IF({TX_SUCCESS}, COALESCE(receipt_l1_gas_used, ({ESTIMATED_SIZE})), 0))",
    ),
    # Gas Fee Paid
    Expression(
        alias="total_gas_fees",
        sql_expr=wei_to_eth(f"SUM({TOTAL_GAS_FEES})"),
    ),
    Expression(
        alias="total_gas_fees_success",
        sql_expr=wei_to_eth(f"SUM(IF({TX_SUCCESS}, {TOTAL_GAS_FEES}, 0))"),
    ),
    # Gas Fee Breakdown
    Expression(
        alias="l2_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L2_CONTRIB_GAS_FEES})"),
    ),
    Expression(
        alias="l1_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L1_CONTRIB_GAS_FEES})"),
    ),
    Expression(
        alias="l1_contrib_contrib_gas_fees_blobgas",
        sql_expr=wei_to_eth(f"SUM({L1_CONTRIB_BLOB})"),
    ),
    Expression(
        alias="l1_contrib_gas_fees_l1gas",
        sql_expr=wei_to_eth(f"SUM({L1_CONTRIB_L1_GAS})"),
    ),
    Expression(
        alias="l2_contrib_gas_fees_basefee",
        sql_expr=wei_to_eth(f"SUM({L2_CONTRIB_BASE})"),
    ),
    Expression(
        alias="l2_contrib_gas_fees_priorityfee",
        sql_expr=wei_to_eth(f"SUM({L2_CONTRIB_PRIORITY})"),
    ),
    Expression(
        alias="l2_contrib_gas_fees_legacyfee",
        sql_expr=wei_to_eth(
            f"SUM(IF(max_priority_fee_per_gas=0, {TOTAL_GAS_FEES} - {L2_CONTRIB_PRIORITY} - {L2_CONTRIB_BASE}, 0))"
        ),
    ),
    # Average Gas Fee
    Expression(
        alias="avg_l2_gas_price_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                f"SUM({L2_CONTRIB_GAS_FEES})",
                "SUM(receipt_gas_used)",
            )
        ),
    ),
    Expression(
        alias="avg_l2_base_fee_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                f"SUM({L2_CONTRIB_BASE})",
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
                f"SUM({L1_CONTRIB_L1_GAS})",
                f"SUM(({ESTIMATED_SIZE}) * COALESCE(16*receipt_l1_base_fee_scalar/1000000, receipt_l1_fee_scalar))",
            )
        ),
    ),
    Expression(
        alias="avg_l1_blob_base_fee_gwei",
        sql_expr=wei_to_gwei(
            safe_div(
                f"SUM({L1_CONTRIB_BLOB})",
                f"SUM(({ESTIMATED_SIZE}) * receipt_l1_blob_base_fee_scalar/1000000)",
            )
        ),
    ),
]


@register_model
def daily_address_summary(
    task: IntermediateModelsTask,
) -> dict[str, duckdb.DuckDBPyRelation]:
    txs: duckdb.DuckDBPyRelation = task.input_duckdb_relations["transactions"]
    blks: duckdb.DuckDBPyRelation = task.input_duckdb_relations["blocks"]

    client = init_client()

    query = f"""
    WITH blocks AS (
        SELECT
            number,
            base_fee_per_gas
        FROM blks
    )
    SELECT
        dt,
        chain,
        chain_id,
        from_address,
        {to_sql(AGGREGATION_EXPRS)}
    FROM txs AS t
    JOIN blocks AS b
        ON t.block_number = b.number
    WHERE gas_price > 0

    -- Optional address filter for faster results when developing.
    AND from_address LIKE '0x00%'

    GROUP BY 1, 2, 3, 4
    """

    # Uncomment when debugging:
    # print(query)

    results = client.sql(query)

    # Model functions always return a dictionary of output results.
    return {"daily_address_summary": results}
