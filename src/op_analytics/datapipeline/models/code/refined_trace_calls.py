# import duckdb

# from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
# from op_analytics.datapipeline.models.compute.registry import register_model
# from op_analytics.datapipeline.models.compute.types import NamedRelations


# @register_model(
#     input_datasets=["ingestion/traces_v1", "refined_transactions_fees_v1"],
#     expected_outputs=["refined_trace_calls_v1"],
#     auxiliary_views=[
#         TemplatedSQLQuery(
#             template_name="refined_trace_calls",
#             context={},
#         ),
#     ],
# )
# def refined_trace_calls(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
#     return {
#         "refined_trace_calls_v1": duckdb_client.view("refined_trace_calls"),
#     }
