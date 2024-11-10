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


# L2 BASE Legacy Fee
L2_BASE_LEGACY = "IF(max_priority_fee_per_gas=0, l2_fee + l1_fee - l2_priority - l2_base, 0)"


# Estimated L1 size of the transaction.
L1_ESTIMATED_SIZE = "l1_fee / l1_weighted_price"

# Estimated gas used (16 per non-zero byte)
L1_ESTIMATED_GAS_USED = f"16 * {L1_ESTIMATED_SIZE}"

# L1 Fee breaakdown into BASE and BLOB contributions
L1_CONTRIB_BASE = f"({L1_ESTIMATED_SIZE}) * l1_base_scaled_price"
L1_CONTRIB_BLOB = f"({L1_ESTIMATED_SIZE}) * l1_blob_scaled_price"


TRANSACTION_CALCULATIONS = [
    # Fees
    Expr(alias="l2_base_legacy", expr=L2_BASE_LEGACY),
    Expr(alias="l1_weighted_gas_price", expr="l1_weighted_price"),
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
            context={},
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
