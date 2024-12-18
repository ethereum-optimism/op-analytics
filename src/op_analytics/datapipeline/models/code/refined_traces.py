from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.datapipeline.models.compute.model import AuxiliaryView, ParquetData
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=[
        "ingestion/blocks_v1",
        "ingestion/traces_v1",
        "ingestion/transactions_v1",
    ],
    expected_outputs=[
        "create_traces_v1",
    ],
    auxiliary_views=[
        "refined_transactions_fees",
        "refined_traces_fees",
    ],
)
def refined_traces(
    ctx: DuckDBContext,
    input_datasets: dict[str, ParquetData],
    auxiliary_views: dict[str, AuxiliaryView],
) -> NamedRelations:
    # Not implemented yet.
    return {}
