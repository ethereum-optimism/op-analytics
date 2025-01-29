import datetime
from datetime import date

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import IntermediateModelTestBase


class TestTokenTransfers001(IntermediateModelTestBase):
    model = "token_transfers"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    dateval = date(2024, 11, 18)
    block_filters = [
        "{block_number} IN (128145990, 128145989) OR block_number % 100 < 2",
    ]

    _enable_fetching = False

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

        assert num_erc20_transfers == 1105
        assert num_erc721_transfers == 35

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
            "dt": "DATE",
            "chain": "VARCHAR",
            "chain_id": "INTEGER",
            "network": "VARCHAR",
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
            "dt": "DATE",
            "chain": "VARCHAR",
            "chain_id": "INTEGER",
            "network": "VARCHAR",
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

    def test_single_tx_erc20(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM erc20_transfers_v1
        WHERE transaction_hash = '0x6ba43fabde9f03dded7c56677751114907201a9e44628b7a64b02d118df25aba'
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "dt": datetime.date(2024, 11, 18),
                "chain": "op",
                "chain_id": 10,
                "network": "mainnet",
                "block_timestamp": 1731890755,
                "block_number": 128145989,
                "block_hash": "0x98f6d6cfb9de5b5ccf9c3d9849bc04ab9a2c4725b6572d5ead0f35787ad4de82",
                "transaction_hash": "0x6ba43fabde9f03dded7c56677751114907201a9e44628b7a64b02d118df25aba",
                "transaction_index": 1,
                "log_index": 0,
                "contract_address": "0xdc6ff44d5d932cbd77b52e5612ba0529dc6226f1",
                "amount": 800000000000000000,
                "amount_lossless": "800000000000000000",
                "from_address": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
                "to_address": "0x73981e74c1b3d94cbe97e2cd03691dd2e7c533fa",
            }
        ]

    def test_single_tx_erc721(self):
        assert self._duckdb_context is not None

        output = (
            self._duckdb_context.client.sql("""
        SELECT * FROM erc721_transfers_v1
        WHERE transaction_hash = '0x8c8097be6a11606a35153d59a846dd0563f461c67a2ab59c5211e00540a4a865'
        """)
            .pl()
            .to_dicts()
        )

        assert output == [
            {
                "dt": datetime.date(2024, 11, 18),
                "chain": "op",
                "chain_id": 10,
                "network": "mainnet",
                "block_timestamp": 1731890757,
                "block_number": 128145990,
                "block_hash": "0x1dbc3f2bc6e28592c242c2da70c30d75cda987cee20f7203e5d99f9d91a9a1d9",
                "transaction_hash": "0x8c8097be6a11606a35153d59a846dd0563f461c67a2ab59c5211e00540a4a865",
                "transaction_index": 8,
                "log_index": 38,
                "contract_address": "0x416b433906b1b72fa758e166e239c43d68dc6f29",
                "from_address": "0x04f0d809f769772138736a1c79daf6b0a7692bb6",
                "to_address": "0xca573537505cce0d03963002e870d74d2cfe8cff",
                "token_id": "554014",
            }
        ]
