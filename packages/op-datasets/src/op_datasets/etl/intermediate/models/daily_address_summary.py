import duckdb

from op_datasets.etl.intermediate.registry import register_model
from op_datasets.etl.intermediate.querybuilder import (
    TemplatedSQLQuery,
    RenderedSQLQuery,
)
from op_datasets.etl.intermediate.types import NamedRelations
from op_datasets.etl.intermediate.udfs import (
    Expr,
)


# L2 Fee and breakdown into BASE and PRIORITY contributions.
L2_FEE = "(gas_price * receipt_gas_used)"
L2_PRIORITY = "(max_priority_fee_per_gas * receipt_gas_used)"
L2_BASE = "(b.base_fee_per_gas * receipt_gas_used)"

# L1 Fee
L1_FEE = "receipt_l1_fee"

# The total fee is the sum of L2 + L1.
TOTAL_FEE = f"{L2_FEE} + {L1_FEE}"

# L2 BASE Legacy Fee
L2_BASE_LEGACY = f"IF(max_priority_fee_per_gas=0, {TOTAL_FEE} - {L2_PRIORITY} - {L2_BASE}, 0)"

# L1 BASE Scaled Price
L1_BASE_SCALAR = "COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar)"
L1_BASE_SCALED_PRICE = f"{L1_BASE_SCALAR} * receipt_l1_gas_price"

# L1 BLOB Scaled Price
L1_BLOB_SCALAR = "micro(receipt_l1_blob_base_fee_scalar)"
L1_BLOB_SCALED_PRICE = f"COALESCE({L1_BLOB_SCALAR} * receipt_l1_blob_base_fee, 0)"

# Estimated L1 size of the transaction.
L1_WEIGHTED_GAS_PRICE = f"({L1_BASE_SCALED_PRICE} + {L1_BLOB_SCALED_PRICE})"
L1_ESTIMATED_SIZE = f"receipt_l1_fee / {L1_WEIGHTED_GAS_PRICE}"

# Estimated gas used (16 per non-zero byte)
L1_ESTIMATED_GAS_USED = f"16 * {L1_ESTIMATED_SIZE}"

# L1 Fee breaakdown into BASE and BLOB contributions
L1_CONTRIB_BASE = f"({L1_ESTIMATED_SIZE}) * {L1_BASE_SCALED_PRICE}"
L1_CONTRIB_BLOB = f"({L1_ESTIMATED_SIZE}) * {L1_BLOB_SCALED_PRICE}"


TRANSACTION_CALCULATIONS = [
    # Fees
    Expr(alias="l2_fee", expr=L2_FEE),
    Expr(alias="l2_priority", expr=L2_PRIORITY),
    Expr(alias="l2_base", expr=L2_BASE),
    Expr(alias="l2_base_legacy", expr=L2_BASE_LEGACY),
    Expr(alias="l1_fee", expr=L1_FEE),
    Expr(alias="total_fee", expr=TOTAL_FEE),
    Expr(alias="l1_base_scalar", expr=L1_BASE_SCALAR),
    Expr(alias="l1_base_scaled_price", expr=L1_BASE_SCALED_PRICE),
    Expr(alias="l1_blob_scalar", expr=L1_BLOB_SCALAR),
    Expr(alias="l1_blob_scaled_price", expr=L1_BLOB_SCALED_PRICE),
    Expr(alias="l1_weighted_gas_price", expr=L1_WEIGHTED_GAS_PRICE),
    Expr(alias="l1_estimated_size", expr=L1_ESTIMATED_SIZE),
    Expr(alias="l1_estimated_gas_used", expr=L1_ESTIMATED_GAS_USED),
    Expr(alias="l1_base", expr=L1_CONTRIB_BASE),
    Expr(alias="l1_blob", expr=L1_CONTRIB_BLOB),
]


@register_model(
    input_datasets=["blocks", "transactions"],
    expected_outputs=["daily_address_summary_v1"],
    query_templates=[
        TemplatedSQLQuery(
            template_name="daily_address_summary",
            result_name="daily_address_summary_v1",
            context={
                "transaction_calculations": TRANSACTION_CALCULATIONS,
            },
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
