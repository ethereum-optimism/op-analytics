import duckdb

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations


@register_model(
    input_datasets=["ingestion/traces_v1", "ingestion/logs_v1", "ingestion/transactions_v1"],
    expected_outputs=["native_transfers_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(
            template_name="native_transfers",
            context={},
        ),
        # TemplatedSQLQuery(
        #     template_name="erc20_transfers",
        #     context={},
        # ),
    ],
)
def token_transfers(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    # Generic Function to decode events with the ABI - Will Prototype in dev notebook?
    # def decode_logs(df):
    # Get Unique Topic0s
    # Grab ABIs for Topic0s - From some log we have? Maybe look up to a repo eventually?
    # Decode the appropriate row based on Topic0 <> ABI Mapping
    # Retrun Decoded Table
    # return
    # Get raw filtered logs for each token standard
    # erc20_view = duckdb_client.view("erc20_transfer_raw_logs")
    # erc721_view = duckdb_client.view("erc721_transfer_raw_logs")
    # erc1155_view = duckdb_client.view("erc1155_transfer_raw_logs")

    # decoded_erc20_view = decode_events(erc20_view, abi)

    return {
        # TBD if we materialize these individually, or roll them up in to a token transfers table
        "native_transfers_v1": duckdb_client.view("native_transfers"),
        # "erc20_transfers_v1": decoded_erc20_view,
    }
