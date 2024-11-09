import duckdb

from op_datasets.etl.intermediate.registry import register_model
from op_datasets.etl.intermediate.querybuilder import (
    TemplatedSQLQuery,
    RenderedSQLQuery,
)
from op_datasets.etl.intermediate.types import NamedRelations
from op_datasets.etl.intermediate.udfs import (
    Expression,
    safe_div,
    wei_to_eth,
    wei_to_gwei,
)

# Reused expressions

BLOCK_HOUR = "epoch_to_hour(block_timestamp)"
TX_SUCCESS = "receipt_status = 1"


# L2 Fee and breakdown into BASE and PRIORITY contributions.
L2_GAS_FEES = "(gas_price * receipt_gas_used)"
L2_CONTRIB_PRIORITY = "(max_priority_fee_per_gas * receipt_gas_used)"
L2_CONTRIB_BASE = "(base_fee_per_gas * receipt_gas_used)"

# L1 Fee
L1_GAS_FEES = "receipt_l1_fee"

# The total fee is the sum of L2 + L1.
TOTAL_GAS_FEES = f"{L2_GAS_FEES} + {L1_GAS_FEES}"

# L1 Fee breakdown into BASE and BLOB contributions.
L1_BASE_SCALAR = "COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar)"
L1_BASE_SCALED_PRICE = f"{L1_BASE_SCALAR} * receipt_l1_gas_price"

L1_BLOB_SCALAR = "micro(receipt_l1_blob_base_fee_scalar)"
L1_BLOB_SCALED_PRICE = f"COALESCE({L1_BLOB_SCALAR} * receipt_l1_blob_base_fee, 0)"

# Estimated L1 size of the transaction.
WEIGHTED_L1_GAS_PRICE = f"({L1_BASE_SCALED_PRICE} + {L1_BLOB_SCALED_PRICE})"
ESTIMATED_L1_SIZE = f"receipt_l1_fee / {WEIGHTED_L1_GAS_PRICE}"

# Estimated gas used (16 per non-zero byte)
ESTIMATED_L1_GAS_USED = f"16 * {ESTIMATED_L1_SIZE}"

# Contributions to L1 Fee
L1_CONTRIB_BASE = f"({ESTIMATED_L1_SIZE}) * {L1_BASE_SCALED_PRICE}"
L1_CONTRIB_BLOB = f"({ESTIMATED_L1_SIZE}) * {L1_BLOB_SCALED_PRICE}"

# Average L1 gas price
AVERAGE_L1_GAS_PRICE = safe_div(
    f"SUM(({ESTIMATED_L1_SIZE}) * {L1_BASE_SCALED_PRICE})",
    f"SUM(({ESTIMATED_L1_SIZE}) * {L1_BASE_SCALAR})",
)

# Average L1 blob fee
AVERAGE_L1_BLOB_FEE = safe_div(
    f"SUM(({ESTIMATED_L1_SIZE}) * {L1_BLOB_SCALED_PRICE})",
    f"SUM(({ESTIMATED_L1_SIZE}) * {L1_BLOB_SCALAR})",
)


AGGREGATION_EXPRS = [
    # Transactions
    Expression(
        alias="total_txs",
        sql_expr="COUNT(hash)",
    ),
    Expression(
        alias="total_txs_success",
        sql_expr=f"COUNT(IF({TX_SUCCESS}, 1, NULL))",
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
    # method ids
    Expression(
        alias="num_method_ids",
        sql_expr="COUNT(DISTINCT SUBSTRING(input,1,10))",
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
        sql_expr=f"SUM(COALESCE(receipt_l1_gas_used, ({ESTIMATED_L1_GAS_USED})))",
    ),
    Expression(
        alias="total_l1_gas_used_success",
        sql_expr=f"SUM(IF({TX_SUCCESS}, COALESCE(receipt_l1_gas_used, ({ESTIMATED_L1_GAS_USED})), 0))",
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
        sql_expr=wei_to_eth(f"SUM({L2_GAS_FEES})"),
    ),
    Expression(
        alias="l1_contrib_gas_fees",
        sql_expr=wei_to_eth(f"SUM({L1_GAS_FEES})"),
    ),
    Expression(
        alias="l1_contrib_contrib_gas_fees_blobgas",
        sql_expr=wei_to_eth(f"SUM({L1_CONTRIB_BLOB})"),
    ),
    Expression(
        alias="l1_contrib_gas_fees_l1gas",
        sql_expr=wei_to_eth(f"SUM({L1_CONTRIB_BASE})"),
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
                f"SUM({L2_GAS_FEES})",
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
        sql_expr=wei_to_gwei(AVERAGE_L1_GAS_PRICE),
    ),
    Expression(
        alias="avg_l1_blob_base_fee_gwei",
        sql_expr=wei_to_gwei(AVERAGE_L1_BLOB_FEE),
    ),
]


@register_model(
    input_datasets=["blocks", "transactions"],
    expected_outputs=["daily_address_summary_v1"],
    query_templates=[
        TemplatedSQLQuery(
            template_name="daily_address_summary",
            result_name="daily_address_summary_v1",
            context={"aggregates": AGGREGATION_EXPRS},
        ),
    ],
)
def daily_address_summary(
    duckdb_client: duckdb.DuckDBPyConnection,
    rendered_queries: dict[str, RenderedSQLQuery],
) -> NamedRelations:
    results = {}
    for name, query in rendered_queries.items():
        # Uncomment when debugging:
        # print(query)

        results[name] = duckdb_client.sql(query.query)

    return results
