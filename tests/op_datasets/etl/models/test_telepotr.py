from datetime import date
import datetime
from decimal import Decimal

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import IntermediateModelTestBase


class TestTeleportr001(IntermediateModelTestBase):
    model = "teleportr"
    inputdata = InputTestData.at(__file__)
    chains = ["worldchain"]
    dateval = date(2024, 12, 8)
    block_filters = [
        "{block_number} > 0",
    ]

    _enable_fetching = True

    def test_row_counts(self):
        """Check row counts from each of the traces and txs results."""

        assert self._duckdb_context is not None

        row_counts = (
            self._duckdb_context.client.sql(
                """
                SELECT COUNT(*) as total FROM bridging_transactions_v1
                """
            )
            .pl()
            .to_dicts()
        )

        assert row_counts == [{"total": 4}]

    def test_bridging_txs_schema(self):
        """Verify the final refined transactions schema."""

        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE bridging_transactions_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "dt": "DATE",
            "block_timestamp": "UINTEGER",
            "block_number": "BIGINT",
            "src_chain": "VARCHAR",
            "src_chain_id": "INTEGER",
            "contract_address": "VARCHAR",
            "transaction_hash": "VARCHAR",
            "deposit_id": "BIGINT",
            "input_token_address": "VARCHAR",
            "output_token_address": "VARCHAR",
            "dst_chain_id": "VARCHAR",
            "input_amount": "BIGINT",
            "output_amount": "BIGINT",
            "quote_timestamp": "BIGINT",
            "fill_deadline": "BIGINT",
            "exclusivity_deadline": "BIGINT",
            "recipient_address": "VARCHAR",
            "relayer_address": "VARCHAR",
            "depositor_address": "VARCHAR",
            "integrator": "VARCHAR",
            "log_index": "BIGINT",
            "l2_fee": "DECIMAL(38,19)",
            "l1_fee": "DECIMAL(38,19)",
            "dst_chain": "VARCHAR",
            "tx_fee": "DECIMAL(38,19)",
        }

    def test_results(self):
        actual = (
            self._duckdb_context.client.sql("SELECT * FROM bridging_transactions_v1")
            .pl()
            .to_dicts()
        )
        assert actual == [
            {
                "dt": datetime.date(2024, 12, 8),
                "block_timestamp": 1733617361,
                "block_number": 7140861,
                "src_chain": "worldchain",
                "src_chain_id": 480,
                "contract_address": "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64",
                "transaction_hash": "0x70070de9133ecca01d01d2bb7f36610a52c6f5c7d5832eca398ac87efe2842bd",
                "deposit_id": 20542,
                "input_token_address": "0x4200000000000000000000000000000000000006",
                "output_token_address": "0x4200000000000000000000000000000000000006",
                "dst_chain_id": "10",
                "input_amount": 247000000000000,
                "output_amount": 246147963394391,
                "quote_timestamp": 1733617115,
                "fill_deadline": 1733631731,
                "exclusivity_deadline": 1733617361,
                "recipient_address": "0x000000000000000000000000f062a5c7f4ecddf8",
                "relayer_address": "0x0000000000000000000000000000000000000000",
                "depositor_address": "0xf062a5c7f4ecddf87173b157f54e0239ea5a685e",
                "integrator": "SuperBridge",
                "log_index": 1,
                "l2_fee": Decimal("6.21989135040E-8"),
                "l1_fee": Decimal("2.142128608870E-7"),
                "dst_chain": "op",
                "tx_fee": Decimal("2.764117743910E-7"),
            },
            {
                "dt": datetime.date(2024, 12, 8),
                "block_timestamp": 1733616825,
                "block_number": 7140593,
                "src_chain": "worldchain",
                "src_chain_id": 480,
                "contract_address": "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64",
                "transaction_hash": "0xe60c4a826951e940767de9d5b25a3327d46e9aac85cecc7a34c9cab462e9a3df",
                "deposit_id": 20541,
                "input_token_address": "0x79a02482a880bce3f13e09da970dc34db4cd24d1",
                "output_token_address": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
                "dst_chain_id": "10",
                "input_amount": 82472000,
                "output_amount": 82437779,
                "quote_timestamp": 1733616635,
                "fill_deadline": 1733631193,
                "exclusivity_deadline": 1733616825,
                "recipient_address": "0x000000000000000000000000caeb5b94c9467c7e",
                "relayer_address": "0x0000000000000000000000000000000000000000",
                "depositor_address": "0xcaeb5b94c9467c7e6ba31cb6bdb78efeaee9d358",
                "integrator": "SuperBridge",
                "log_index": 1,
                "l2_fee": Decimal("7.43192093280E-8"),
                "l1_fee": Decimal("2.627774258960E-7"),
                "dst_chain": "op",
                "tx_fee": Decimal("3.370966352240E-7"),
            },
            {
                "dt": datetime.date(2024, 12, 8),
                "block_timestamp": 1733619567,
                "block_number": 7141964,
                "src_chain": "worldchain",
                "src_chain_id": 480,
                "contract_address": "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64",
                "transaction_hash": "0x77a59bb6824552f40ab2040f44ad474321ee99413ea1ce618dcf3ebfdd2d1462",
                "deposit_id": 20545,
                "input_token_address": "0x4200000000000000000000000000000000000006",
                "output_token_address": "0x4200000000000000000000000000000000000006",
                "dst_chain_id": "10",
                "input_amount": 5480000000000000,
                "output_amount": 5472886127129322,
                "quote_timestamp": 1733619275,
                "fill_deadline": 1733633921,
                "exclusivity_deadline": 1733619567,
                "recipient_address": "0x0000000000000000000000007b1be6c8dbef3874",
                "relayer_address": "0x0000000000000000000000000000000000000000",
                "depositor_address": "0x7b1be6c8dbef38746db2a9c303e42b3e02597ab4",
                "integrator": "SuperBridge",
                "log_index": 2,
                "l2_fee": Decimal("6.34501200000E-8"),
                "l1_fee": Decimal("2.386642398700E-7"),
                "dst_chain": "op",
                "tx_fee": Decimal("3.021143598700E-7"),
            },
            {
                "dt": datetime.date(2024, 12, 8),
                "block_timestamp": 1733619183,
                "block_number": 7141772,
                "src_chain": "worldchain",
                "src_chain_id": 480,
                "contract_address": "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64",
                "transaction_hash": "0x3955c5b3adfc27d88dd4905897c7efe2ed37bb84bca6ceb41130f022f08a7e54",
                "deposit_id": 20544,
                "input_token_address": "0x4200000000000000000000000000000000000006",
                "output_token_address": "0x4200000000000000000000000000000000000006",
                "dst_chain_id": "10",
                "input_amount": 2998300000000000,
                "output_amount": 2994181358695443,
                "quote_timestamp": 1733619035,
                "fill_deadline": 1733633535,
                "exclusivity_deadline": 1733619183,
                "recipient_address": "0x0000000000000000000000001080034b7c55bb9d",
                "relayer_address": "0x0000000000000000000000000000000000000000",
                "depositor_address": "0x1080034b7c55bb9dd8677f0315ca2c3cf281d9e8",
                "integrator": "SuperBridge",
                "log_index": 8,
                "l2_fee": Decimal("6.22231066500E-8"),
                "l1_fee": Decimal("2.189177507670E-7"),
                "dst_chain": "op",
                "tx_fee": Decimal("2.811408574170E-7"),
            },
        ]
