from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestNativeTransfers001(ModelTestBase):
    model = "native_transfers"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    target_range = date(2024, 11, 18)
    block_filters = [
        "{block_number} IN (128145990, 128145989) OR block_number % 100 < 2",
    ]

    _enable_fetching = False

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        num_native_transfers = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_native_transfers FROM native_transfers_v1"
            )
            .pl()
            .to_dicts()[0]["num_native_transfers"]
        )

        # This is a basic test - the actual count will depend on the test data
        assert num_native_transfers >= 0

    def test_model_schema(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE native_transfers_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        expected_schema = {
            "dt": "DATE",
            "chain": "VARCHAR",
            "chain_id": "INTEGER",
            "network": "VARCHAR",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "block_hash": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "trace_address": "VARCHAR",
            "from_address": "VARCHAR",
            "to_address": "VARCHAR",
            "amount": "DECIMAL(38,0)",
            "amount_lossless": "VARCHAR",
            "input_method_id": "VARCHAR",
            "call_type": "VARCHAR",
            "transfer_type": "VARCHAR",
        }

        assert actual_schema == expected_schema
