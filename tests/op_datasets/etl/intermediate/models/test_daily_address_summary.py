from datetime import date
from decimal import Decimal

from op_coreutils.testutils.inputdata import InputTestData
from op_datasets.etl.intermediate.testutils import IntermediateModelTestBase


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

    _enable_fetching = True

    def test_system_address_not_included(self):
        ans = self._duckdb_client.sql(f"""
        SELECT * FROM daily_address_summary_v1 WHERE address = '{SYSTEM_ADDRESS}'
        """)

        assert len(ans) == 0

    def test_uniqueness(self):
        unique_count = self._duckdb_client.sql("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT address, chain_id, dt FROM daily_address_summary_v1
        )
        """)
        total_count = self._duckdb_client.sql("""
        SELECT COUNT(*) FROM daily_address_summary_v1
        """)

        unique_count_val = unique_count.fetchone()[0]
        total_count_val = total_count.fetchone()[0]

        assert unique_count_val == 785
        assert unique_count_val == total_count_val

    def test_model_schema(self):
        schema = (
            self._duckdb_client.sql("DESCRIBE daily_address_summary_v1")
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
            "total_txs": "BIGINT",
            "total_txs_success": "BIGINT",
            "total_blocks": "BIGINT",
            "total_blocks_success": "BIGINT",
            "min_block_number": "BIGINT",
            "max_block_number": "BIGINT",
            "block_interval_active": "BIGINT",
            "min_nonce": "BIGINT",
            "max_nonce": "BIGINT",
            "nonce_interval_active": "BIGINT",
            "min_block_timestamp": "UINTEGER",
            "max_block_timestamp": "UINTEGER",
            "time_interval_active": "UINTEGER",
            "unique_hours_active": "BIGINT",
            "num_to_addresses": "BIGINT",
            "num_to_addresses_success": "BIGINT",
            "num_method_ids": "BIGINT",
            "total_l2_gas_used": "DECIMAL(38,0)",
            "total_l2_gas_used_success": "DECIMAL(38,0)",
            # TODO: Should l1_gas_used values be DECIMAL(38,0) instead of DOUBLE ???
            "total_l1_gas_used": "DOUBLE",
            "total_l1_gas_used_success": "DOUBLE",
            "total_gas_fees": "DECIMAL(38,19)",
            "total_gas_fees_success": "DECIMAL(38,19)",
            "l2_contrib_gas_fees": "DECIMAL(38,19)",
            "l1_contrib_gas_fees": "DECIMAL(38,19)",
            "l1_contrib_contrib_gas_fees_blobgas": "DECIMAL(38,19)",
            "l1_contrib_gas_fees_l1gas": "DECIMAL(38,19)",
            "l2_contrib_gas_fees_basefee": "DECIMAL(38,19)",
            "l2_contrib_gas_fees_priorityfee": "DECIMAL(38,19)",
            "l2_contrib_gas_fees_legacyfee": "DECIMAL(38,19)",
            "avg_l2_gas_price_gwei": "DECIMAL(38,10)",
            "avg_l2_base_fee_gwei": "DECIMAL(38,10)",
            "avg_l2_priority_fee_gwei": "DECIMAL(38,10)",
            "avg_l1_gas_price_gwei": "DECIMAL(38,10)",
            "avg_l1_blob_base_fee_gwei": "DECIMAL(38,10)",
        }

    def test_single_txs_output(self):
        # Transaction that is relevant for this test:
        # https://optimistic.etherscan.io/tx/0x2ab7a335f3ecc0236ac9fc0c4832f4500d299ee40abf99e5609fa16309f82763

        actual = (
            self._duckdb_client.sql(f"""
        SELECT * FROM daily_address_summary_v1 WHERE address == '{SINGLE_TX_ADDRESS}'
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
                "total_txs": 1,
                "total_txs_success": 1,
                "total_blocks": 1,
                "total_blocks_success": 1,
                "min_block_number": 126078000,
                "max_block_number": 126078000,
                "block_interval_active": 1,
                "min_nonce": 52,
                "max_nonce": 52,
                "nonce_interval_active": 1,
                "min_block_timestamp": 1727754777,
                "max_block_timestamp": 1727754777,
                "time_interval_active": 0,
                "unique_hours_active": 1,
                "num_to_addresses": 1,
                "num_to_addresses_success": 1,
                "num_method_ids": 1,
                "total_l2_gas_used": Decimal("47038"),
                "total_l2_gas_used_success": Decimal("47038"),
                "total_l1_gas_used": 1600.0,
                "total_l1_gas_used_success": 1600.0,
                "total_gas_fees": Decimal("2.723088381200E-7"),
                "total_gas_fees_success": Decimal("2.723088381200E-7"),
                "l2_contrib_gas_fees": Decimal("2.59593784780E-8"),
                "l1_contrib_gas_fees": Decimal("2.463494596420E-7"),
                "l1_contrib_contrib_gas_fees_blobgas": Decimal("1.010E-16"),
                "l1_contrib_gas_fees_l1gas": Decimal("2.463494595410E-7"),
                "l2_contrib_gas_fees_basefee": Decimal("2.12555784780E-8"),
                "l2_contrib_gas_fees_priorityfee": Decimal("4.7038000000E-9"),
                "l2_contrib_gas_fees_legacyfee": Decimal("0E-19"),
                "avg_l2_gas_price_gwei": Decimal("0.0005518810"),
                "avg_l2_base_fee_gwei": Decimal("0.0004518810"),
                "avg_l2_priority_fee_gwei": Decimal("0.0001000000"),
                "avg_l1_gas_price_gwei": Decimal("29.4563635380"),
                "avg_l1_blob_base_fee_gwei": Decimal("1.0E-9"),
            }
        ]

        assert (
            sum(
                [
                    actual[0]["l1_contrib_gas_fees"],
                    actual[0]["l2_contrib_gas_fees"],
                ]
            )
            == actual[0]["total_gas_fees"]
        )

        assert (
            sum(
                [
                    actual[0]["l1_contrib_contrib_gas_fees_blobgas"],
                    actual[0]["l1_contrib_gas_fees_l1gas"],
                ]
            )
            == actual[0]["l1_contrib_gas_fees"]
        )

        assert (
            sum(
                [
                    actual[0]["l2_contrib_gas_fees_basefee"],
                    actual[0]["l2_contrib_gas_fees_priorityfee"],
                    actual[0]["l2_contrib_gas_fees_legacyfee"],
                ]
            )
            == actual[0]["l2_contrib_gas_fees"]
        )

    def test_multiple_txs_output(self):
        # Transactions that are relevant for this test:
        # https://optimistic.etherscan.io/tx/0x0c8c81cc9a97f4d66f4f78ef2bbb5a64c23358db2c4ba6ad35e338f2d2fa3535
        # https://optimistic.etherscan.io/tx/0x8aa91fc3fb1c11cd4aba16130697a1fa52fd74ae7ee9f23627b6b8b42fec0a34

        actual = (
            self._duckdb_client.sql(f"""
        SELECT * FROM daily_address_summary_v1 WHERE address == '{MULTI_TXS_ADDRESS}'
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
                "total_txs": 2,
                "total_txs_success": 2,
                "total_blocks": 2,
                "total_blocks_success": 2,
                "min_block_number": 126073799,
                "max_block_number": 126090026,
                "block_interval_active": 16228,
                "min_nonce": 54,
                "max_nonce": 55,
                "nonce_interval_active": 2,
                "min_block_timestamp": 1727746375,
                "max_block_timestamp": 1727778829,
                "time_interval_active": 32454,
                "unique_hours_active": 2,
                "num_to_addresses": 1,
                "num_to_addresses_success": 1,
                "num_method_ids": 1,
                "total_l2_gas_used": Decimal("59852"),
                "total_l2_gas_used_success": Decimal("59852"),
                "total_l1_gas_used": 3200.0,
                "total_l1_gas_used_success": 3200.0,
                "total_gas_fees": Decimal("0.0000037408546364060"),
                "total_gas_fees_success": Decimal("0.0000037408546364060"),
                "l2_contrib_gas_fees": Decimal("0.0000035911200000000"),
                "l1_contrib_gas_fees": Decimal("1.497346364060E-7"),
                "l1_contrib_contrib_gas_fees_blobgas": Decimal("2.030E-16"),
                "l1_contrib_gas_fees_l1gas": Decimal("1.497346362030E-7"),
                "l2_contrib_gas_fees_basefee": Decimal("2.38530270420E-8"),
                "l2_contrib_gas_fees_priorityfee": Decimal("0E-19"),
                "l2_contrib_gas_fees_legacyfee": Decimal("0.0000037170016093640"),
                "avg_l2_gas_price_gwei": Decimal("0.0600000000"),
                "avg_l2_base_fee_gwei": Decimal("0.0003985340"),
                "avg_l2_priority_fee_gwei": Decimal("0E-10"),
                "avg_l1_gas_price_gwei": Decimal("8.9519942250"),
                "avg_l1_blob_base_fee_gwei": Decimal("1.0E-9"),
            }
        ]

        assert actual[0]["block_interval_active"] == (
            actual[0]["max_block_number"] - actual[0]["min_block_number"] + 1
        )

        assert actual[0]["nonce_interval_active"] == (
            actual[0]["max_nonce"] - actual[0]["min_nonce"] + 1
        )

        assert actual[0]["time_interval_active"] == (
            actual[0]["max_block_timestamp"] - actual[0]["min_block_timestamp"]
        )

    def test_overall_totals(self):
        actual = (
            self._duckdb_client.sql("SELECT SUM(total_txs) FROM daily_address_summary_v1")
            .pl()
            .to_dicts()
        )
        assert actual == [{"sum(total_txs)": Decimal("2397")}]

    def test_consistency(self):
        actual = (
            self._duckdb_client.sql("""
            WITH checks AS (
                SELECT
                    true                                                      AS check0,

                    l1_contrib_gas_fees
                    + l2_contrib_gas_fees = total_gas_fees                    AS check1,

                    l1_contrib_contrib_gas_fees_blobgas
                    + l1_contrib_gas_fees_l1gas = l1_contrib_gas_fees         AS check2,

                    l2_contrib_gas_fees_basefee
                    + l2_contrib_gas_fees_priorityfee
                    + l2_contrib_gas_fees_legacyfee  = l2_contrib_gas_fees    AS check3,

                    block_interval_active =
                    max_block_number - min_block_number + 1                   AS check4,

                    nonce_interval_active =
                    max_nonce - min_nonce + 1                                 AS check5,

                    time_interval_active =
                    max_block_timestamp - min_block_timestamp                 AS check6

                FROM daily_address_summary_v1
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
