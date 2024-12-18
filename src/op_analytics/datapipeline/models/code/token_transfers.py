import duckdb
from duckdb.typing import VARCHAR, BIGINT  # Import DuckDB's type annotations

from op_analytics.datapipeline.models.compute.querybuilder import TemplatedSQLQuery
from op_analytics.datapipeline.models.compute.registry import register_model
from op_analytics.datapipeline.models.compute.types import NamedRelations
from eth_abi import decode


@register_model(
    input_datasets=["ingestion/traces_v1", "ingestion/logs_v1", "ingestion/transactions_v1"],
    expected_outputs=["native_transfers_v1", "erc20_transfers_v1"],
    auxiliary_views=[
        TemplatedSQLQuery(template_name="native_transfers", context={}),
        TemplatedSQLQuery(template_name="filtered_raw_erc20_transfer_logs", context={}),
    ],
)
def token_transfers(duckdb_client: duckdb.DuckDBPyConnection) -> NamedRelations:
    def decode_erc20_transfer(topics: str, data: str):
        """
        Decode ERC-20 transfer log data into structured fields.
        :param topics: JSON-like string representing log topics.
        :param data: Hexadecimal string of ABI-encoded data.
        :return: Tuple of (from_address, to_address, value).
        """
        import json

        topics = json.loads(topics)  # Convert JSON string to list
        if len(topics) < 3:
            raise ValueError("Insufficient topics to decode transfer.")

        from_address = decode(["address"], bytes.fromhex(topics[1][2:]))[0]
        to_address = decode(["address"], bytes.fromhex(topics[2][2:]))[0]
        value = decode(["uint256"], bytes.fromhex(data[2:]))[0]
        return from_address, to_address, value

    # Register the function in DuckDB
    duckdb_client.create_function(
        "decode_erc20_transfer",
        decode_erc20_transfer,
        parameters=[VARCHAR, VARCHAR],  # Input types
        return_type=[VARCHAR, VARCHAR, BIGINT],  # Output types: 3 separate columns
    )

    erc20_logs_decoded = duckdb_client.view("""
        SELECT 
            decode_erc20_transfer(topics, data).from_address AS from_address,
            decode_erc20_transfer(topics, data).to_address AS to_address,
            decode_erc20_transfer(topics, data).value AS value
        FROM filtered_raw_erc20_transfer_logs l
    """)

    return {
        "native_transfers_v1": duckdb_client.view("native_transfers"),
        "erc20_transfers_v1": erc20_logs_decoded,
    }
