import duckdb

from op_analytics.datapipeline.etl.intermediate.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.etl.intermediate.registry import register_model
from op_analytics.datapipeline.etl.intermediate.types import NamedRelations


@register_model(
    input_datasets=["traces", "transactions"],
    expected_outputs=["creation_traces_v1"],
    duckdb_views=[
        TemplatedSQLQuery(
            template_name="creation_traces",
            context={},
        ),
    ],
    # block_filter_pct=2,  # <-- development mode
)
def creation_traces(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    return {
        "creation_traces_v1": duckdb_client.sql("SELECT * FROM creation_traces"),
    }
