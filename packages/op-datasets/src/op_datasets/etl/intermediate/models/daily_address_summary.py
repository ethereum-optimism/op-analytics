import duckdb

from op_datasets.etl.intermediate.registry import register_model
from op_datasets.etl.intermediate.querybuilder import (
    TemplatedSQLQuery,
    RenderedSQLQuery,
)
from op_datasets.etl.intermediate.types import NamedRelations


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
