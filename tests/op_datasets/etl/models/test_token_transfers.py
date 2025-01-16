from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import IntermediateModelTestBase


class TestTokenTransfers001(IntermediateModelTestBase):
    model = "token_transfers"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    dateval = date(2024, 11, 18)
    datasets = ["logs"]
    block_filters = [
        "{block_number} > 0",
    ]

    _enable_fetching = True

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        num_erc20_transfers = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_erc20_transfers FROM erc20_transfers_v1"
            )
            .pl()
            .to_dicts()[0]["num_erc20_transfers"]
        )

        num_erc721_transfers = (
            self._duckdb_context.client.sql(
                "SELECT COUNT(*) as num_erc721_transfers FROM erc721_transfers_v1"
            )
            .pl()
            .to_dicts()[0]["num_erc721_transfers"]
        )

        assert num_erc20_transfers == 60981
        assert num_erc721_transfers == 1998

    def test_model_schema_erc20(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE erc20_transfers_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "chain_id": "INTEGER",
            "chain": "VARCHAR",
            "dt": "DATE",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "block_hash": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "log_index": "BIGINT",
            "contract_address": "VARCHAR",
            "amount": "UBIGINT",
            "amount_lossless": "VARCHAR",
            "from_address": "VARCHAR",
            "to_address": "VARCHAR",
        }

    def test_model_schema_er721(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE erc721_transfers_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "chain_id": "INTEGER",
            "chain": "VARCHAR",
            "dt": "DATE",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "block_hash": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "log_index": "BIGINT",
            "contract_address": "VARCHAR",
            "from_address": "VARCHAR",
            "to_address": "VARCHAR",
            "token_id": "VARCHAR",
        }
