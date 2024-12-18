from datetime import date
from decimal import Decimal

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import IntermediateModelTestBase


SYSTEM_ADDRESS = "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001"
SINGLE_TX_ADDRESS = "0xd666115c3d251bece7896297bf446ea908caf035"
MULTI_TXS_ADDRESS = "0xcef6d40144b0d76617664357a15559ecb145374f"

INPUT_TEST_DATA = InputTestData.at(__file__)


class TestDailyAddressSummary001(IntermediateModelTestBase):
    model = "daily_address_summary"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    dateval = date(2024, 10, 1)
    datasets = ["blocks", "transactions"]
    block_filters = [
        "{block_number} = 126078000",
        "{block_number} = 126073799",
        "{block_number} = 126090026",
        "{block_number} % 1000 <= 2",
    ]

    _enable_fetching = False

    def test_system_address_not_included(self):
        assert self._duckdb_context is not None

        ans = self._duckdb_context.client.sql(f"""
        SELECT * FROM summary_v1 WHERE address = '{SYSTEM_ADDRESS}'
        """)

        assert len(ans) == 0

    def test_uniqueness(self):
        assert self._duckdb_context is not None

        unique_count = self._duckdb_context.client.sql("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT address, chain_id, dt FROM summary_v1
        )
        """)
        total_count = self._duckdb_context.client.sql("""
        SELECT COUNT(*) FROM summary_v1
        """)

        unique_count_val = (unique_count.fetchone() or [None])[0]
        total_count_val = (total_count.fetchone() or [None])[0]

        assert unique_count_val == 785
        assert unique_count_val == total_count_val

    def test_model_schema(self):
        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE summary_v1")
            .pl()
            .select("column_name", "column_type")
            .to_dicts()
        )
        actual_schema = {row["column_name"]: row["column_type"] for row in schema}

        assert actual_schema == {
            "dt": "DATE",
            "chain": "VARCHAR",
            "chain_id": "INTEGER",
            "address": "VARCHAR",
            "tx_cnt": "BIGINT",
            "success_tx_cnt": "BIGINT",
            "block_cnt": "BIGINT",
            "success_block_cnt": "BIGINT",
            "block_number_min": "BIGINT",
            "block_number_max": "BIGINT",
            "active_block_range": "BIGINT",
            "nonce_min": "BIGINT",
            "nonce_max": "BIGINT",
            "active_nonce_range": "BIGINT",
            "block_timestamp_min": "UINTEGER",
            "block_timestamp_max": "UINTEGER",
            "active_time_range": "UINTEGER",
            "active_hours_ucnt": "BIGINT",
            "to_address_ucnt": "BIGINT",
            "success_to_address_ucnt": "BIGINT",
            "method_id_ucnt": "BIGINT",
            "l2_gas_used_sum": "DECIMAL(38,0)",
            "success_l2_gas_used_sum": "DECIMAL(38,0)",
            "l1_gas_used_sum": "DECIMAL(38,0)",
            "success_l1_gas_used_sum": "DECIMAL(38,0)",
            "tx_fee_sum_eth": "DECIMAL(38,19)",
            "success_tx_fee_sum_eth": "DECIMAL(38,19)",
            "l2_fee_sum_eth": "DECIMAL(38,19)",
            "l2_base_fee_sum_eth": "DECIMAL(38,19)",
            "l2_priority_fee_sum_eth": "DECIMAL(38,19)",
            "l2_base_legacy_fee_sum_eth": "DECIMAL(38,19)",
            "l1_fee_sum_eth": "DECIMAL(38,19)",
            "l1_base_fee_sum_eth": "DECIMAL(38,19)",
            "l1_blob_fee_sum_eth": "DECIMAL(38,19)",
            "l2_gas_price_avg_gwei": "DECIMAL(38,10)",
            "l2_base_price_avg_gwei": "DECIMAL(38,10)",
            "l2_priority_price_avg_gwei": "DECIMAL(38,10)",
            "l1_base_price_avg_gwei": "DECIMAL(38,10)",
            "l1_blob_fee_avg_gwei": "DECIMAL(38,10)",
        }

    def test_single_txs_output(self):
        assert self._duckdb_context is not None

        # Transaction that is relevant for this test:
        # https://optimistic.etherscan.io/tx/0x2ab7a335f3ecc0236ac9fc0c4832f4500d299ee40abf99e5609fa16309f82763

        actual = (
            self._duckdb_context.client.sql(f"""
        SELECT * FROM summary_v1 WHERE address == '{SINGLE_TX_ADDRESS}'
        """)
            .pl()
            .to_dicts()
        )

        assert actual == [
            {
                "dt": date(2024, 10, 1),
                "chain": "op",
                "chain_id": 10,
                "address": "0xd666115c3d251bece7896297bf446ea908caf035",
                "tx_cnt": 1,
                "success_tx_cnt": 1,
                "block_cnt": 1,
                "success_block_cnt": 1,
                "block_number_min": 126078000,
                "block_number_max": 126078000,
                "active_block_range": 1,
                "nonce_min": 52,
                "nonce_max": 52,
                "active_nonce_range": 1,
                "block_timestamp_min": 1727754777,
                "block_timestamp_max": 1727754777,
                "active_time_range": 0,
                "active_hours_ucnt": 1,
                "to_address_ucnt": 1,
                "success_to_address_ucnt": 1,
                "method_id_ucnt": 1,
                "l2_gas_used_sum": Decimal("47038"),
                "success_l2_gas_used_sum": Decimal("47038"),
                "l1_gas_used_sum": 1600.0,
                "success_l1_gas_used_sum": 1600.0,
                "tx_fee_sum_eth": Decimal("2.723088381200E-7"),
                "success_tx_fee_sum_eth": Decimal("2.723088381200E-7"),
                "l2_fee_sum_eth": Decimal("2.59593784780E-8"),
                "l2_base_fee_sum_eth": Decimal("2.12555784780E-8"),
                "l2_priority_fee_sum_eth": Decimal("4.7038000000E-9"),
                "l2_base_legacy_fee_sum_eth": Decimal("0E-19"),
                "l1_fee_sum_eth": Decimal("2.463494596420E-7"),
                "l1_base_fee_sum_eth": Decimal("2.463494595410E-7"),
                "l1_blob_fee_sum_eth": Decimal("1.010E-16"),
                "l2_gas_price_avg_gwei": Decimal("0.0005518810"),
                "l2_base_price_avg_gwei": Decimal("0.0004518810"),
                "l2_priority_price_avg_gwei": Decimal("0.0001000000"),
                "l1_base_price_avg_gwei": Decimal("29.4563635380"),
                "l1_blob_fee_avg_gwei": Decimal("1.0E-9"),
            }
        ]

        assert (
            sum(
                [
                    actual[0]["l1_fee_sum_eth"],
                    actual[0]["l2_fee_sum_eth"],
                ]
            )
            == actual[0]["tx_fee_sum_eth"]
        )

        assert (
            sum(
                [
                    actual[0]["l1_blob_fee_sum_eth"],
                    actual[0]["l1_base_fee_sum_eth"],
                ]
            )
            == actual[0]["l1_fee_sum_eth"]
        )

        assert (
            sum(
                [
                    actual[0]["l2_base_fee_sum_eth"],
                    actual[0]["l2_priority_fee_sum_eth"],
                    actual[0]["l2_base_legacy_fee_sum_eth"],
                ]
            )
            == actual[0]["l2_fee_sum_eth"]
        )

    def test_multiple_txs_output(self):
        assert self._duckdb_context is not None

        # Transactions that are relevant for this test:
        # https://optimistic.etherscan.io/tx/0x0c8c81cc9a97f4d66f4f78ef2bbb5a64c23358db2c4ba6ad35e338f2d2fa3535
        # https://optimistic.etherscan.io/tx/0x8aa91fc3fb1c11cd4aba16130697a1fa52fd74ae7ee9f23627b6b8b42fec0a34

        actual = (
            self._duckdb_context.client.sql(f"""
        SELECT * FROM summary_v1 WHERE address == '{MULTI_TXS_ADDRESS}'
        """)
            .pl()
            .to_dicts()
        )

        assert actual == [
            {
                "dt": date(2024, 10, 1),
                "chain": "op",
                "chain_id": 10,
                "address": "0xcef6d40144b0d76617664357a15559ecb145374f",
                "tx_cnt": 2,
                "success_tx_cnt": 2,
                "block_cnt": 2,
                "success_block_cnt": 2,
                "block_number_min": 126073799,
                "block_number_max": 126090026,
                "active_block_range": 16228,
                "nonce_min": 54,
                "nonce_max": 55,
                "active_nonce_range": 2,
                "block_timestamp_min": 1727746375,
                "block_timestamp_max": 1727778829,
                "active_time_range": 32454,
                "active_hours_ucnt": 2,
                "to_address_ucnt": 1,
                "success_to_address_ucnt": 1,
                "method_id_ucnt": 1,
                "l2_gas_used_sum": Decimal("59852"),
                "success_l2_gas_used_sum": Decimal("59852"),
                "l1_gas_used_sum": Decimal("3200"),
                "success_l1_gas_used_sum": Decimal("3200"),
                "tx_fee_sum_eth": Decimal("0.0000037408546364060"),
                "success_tx_fee_sum_eth": Decimal("0.0000037408546364060"),
                "l2_fee_sum_eth": Decimal("0.0000035911200000000"),
                "l2_base_fee_sum_eth": Decimal("2.38530270420E-8"),
                "l2_priority_fee_sum_eth": Decimal("0E-19"),
                "l2_base_legacy_fee_sum_eth": Decimal("0.0000035672669729580"),
                "l1_fee_sum_eth": Decimal("1.497346364060E-7"),
                "l1_base_fee_sum_eth": Decimal("1.497346362050E-7"),
                "l1_blob_fee_sum_eth": Decimal("2.030E-16"),
                "l2_gas_price_avg_gwei": Decimal("0.0600000000"),
                "l2_base_price_avg_gwei": Decimal("0.0003985340"),
                "l2_priority_price_avg_gwei": Decimal("0E-10"),
                "l1_base_price_avg_gwei": Decimal("8.9519942250"),
                "l1_blob_fee_avg_gwei": Decimal("1.0E-9"),
            }
        ]

        assert actual[0]["active_block_range"] == (
            actual[0]["block_number_max"] - actual[0]["block_number_min"] + 1
        )

        assert actual[0]["active_nonce_range"] == (
            actual[0]["nonce_max"] - actual[0]["nonce_min"] + 1
        )

        assert actual[0]["active_time_range"] == (
            actual[0]["block_timestamp_max"] - actual[0]["block_timestamp_min"]
        )

        # TODO: Finish this after fixing types to DECIMAL
        # total_l1_fee = actual[0]["total_l1_gas_used"] * actual[0]["avg_l1_gas_price_gwei"]
        # l1_fee_sum_eth = actual[0]["l1_fee_sum_eth"]
        # assert total_l1_fee == l1_fee_sum_eth

    def test_overall_totals(self):
        assert self._duckdb_context is not None

        actual = (
            self._duckdb_context.client.sql("SELECT SUM(tx_cnt) FROM summary_v1").pl().to_dicts()
        )
        assert actual == [{"sum(tx_cnt)": Decimal("2397")}]

    def test_consistency(self):
        assert self._duckdb_context is not None

        actual = (
            self._duckdb_context.client.sql("""
            WITH checks AS (
                SELECT
                    true                                             AS check0,

                    l1_fee_sum_eth
                    + l2_fee_sum_eth = tx_fee_sum_eth                AS check1,

                    l1_blob_fee_sum_eth
                    + l1_base_fee_sum_eth = l1_fee_sum_eth           AS check2,

                    l2_base_fee_sum_eth
                    + l2_priority_fee_sum_eth
                    + l2_base_legacy_fee_sum_eth  = l2_fee_sum_eth   AS check3,

                    active_block_range =
                    block_number_max - block_number_min + 1          AS check4,

                    active_nonce_range =
                    nonce_max - nonce_min + 1                        AS check5,

                    active_time_range =
                    block_timestamp_max - block_timestamp_min        AS check6

                FROM summary_v1
            )

            SELECT
                count(check0),
                count(check1),
                count(check2),
                count(check3),
                count(check4),
                count(check5),
                count(check6)
            FROM checks
            """)
            .pl()
            .to_dicts()
        )

        assert actual == [
            {
                "count(check0)": 785,
                "count(check1)": 785,
                "count(check2)": 785,
                "count(check3)": 785,
                "count(check4)": 785,
                "count(check5)": 785,
                "count(check6)": 785,
            }
        ]
