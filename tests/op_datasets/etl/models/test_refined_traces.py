from datetime import date
from decimal import Decimal

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestRefinedTraces001(ModelTestBase):
    model = "refined_traces"
    inputdata = InputTestData.at(__file__)
    chains = ["op"]
    target_range = date(2024, 11, 18)
    block_filters = [
        "{block_number} % 200 <= 1",
    ]

    _enable_fetching = False

    def test_row_counts(self):
        """Check row counts from each of the traces and txs results."""

        assert self._duckdb_context is not None

        row_counts = (
            self._duckdb_context.client.sql(
                """
                SELECT 'traces' as name, COUNT(*) as total FROM refined_traces_fees_v1
                
                UNION ALL
                
                SELECT 'txs' as name, COUNT(*) as total FROM refined_transactions_fees_v1
                
                """
            )
            .pl()
            .to_dicts()
        )

        assert row_counts == [
            {"name": "traces", "total": 12508},
            {"name": "txs", "total": 256},
        ]

    def test_refined_txs_schema(self):
        """Verify the final refined transactions schema."""

        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE refined_transactions_fees_v1")
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
            "nonce": "BIGINT",
            "transaction_index": "BIGINT",
            "from_address": "VARCHAR",
            "to_address": "VARCHAR",
            "block_number": "BIGINT",
            "block_timestamp": "UINTEGER",
            "hash": "VARCHAR",
            "transaction_type": "INTEGER",
            "gas_price": "BIGINT",
            "gas_limit": "BIGINT",
            "l2_gas_used": "BIGINT",
            "receipt_l1_gas_used": "BIGINT",
            "l1_fee": "BIGINT",
            "l1_gas_price": "BIGINT",
            "l1_blob_base_fee": "BIGINT",
            "base_fee_per_gas": "BIGINT",
            "max_priority_fee_per_gas": "BIGINT",
            "l1_fee_scalar": "DECIMAL(12,6)",
            "l1_base_fee_scalar": "DECIMAL(26,7)",
            "l1_blob_base_fee_scalar": "DECIMAL(26,7)",
            "legacy_extra_fee_per_gas": "BIGINT",
            "l2_fee": "BIGINT",
            "l2_priority_fee": "BIGINT",
            "l2_base_fee": "BIGINT",
            "method_id": "VARCHAR",
            "success": "BOOLEAN",
            "input_byte_length": "INTEGER",
            "input_zero_bytes": "INTEGER",
            "is_system_transaction": "BOOLEAN",
            "is_attributes_deposited_transaction": "BOOLEAN",
            "block_hour": "TIMESTAMP",
            "estimated_size": "BIGINT",
            "l1_gas_used_unified": "BIGINT",
            "tx_fee": "BIGINT",
            "l2_legacy_extra_fee": "BIGINT",
            "l1_base_fee": "DECIMAL(38,12)",
            "l1_base_scaled_size": "DECIMAL(38,12)",
            "l1_blob_fee": "DECIMAL(38,12)",
            "l1_blob_scaled_size": "DECIMAL(38,12)",
            "tx_fee_native": "DECIMAL(38,19)",
            "l1_fee_native": "DECIMAL(38,19)",
            "l2_fee_native": "DECIMAL(38,19)",
            "l1_base_fee_native": "DECIMAL(38,19)",
            "l1_blob_fee_native": "DECIMAL(38,19)",
            "l2_base_fee_native": "DECIMAL(38,19)",
            "l2_priority_fee_native": "DECIMAL(38,19)",
            "l2_legacy_extra_fee_native": "DECIMAL(38,19)",
            "l2_gas_price_gwei": "DECIMAL(38,10)",
            "l2_base_gas_price_gwei": "DECIMAL(38,10)",
            "l2_priority_gas_price_gwei": "DECIMAL(38,10)",
            "l2_legacy_extra_gas_price_gwei": "DECIMAL(38,10)",
            "l1_base_gas_price_gwei": "DECIMAL(38,10)",
            "l1_blob_base_gas_price_gwei": "DECIMAL(38,10)",
        }

    def test_refined_traces_schema(self):
        """Verify the final refined traces schema."""

        assert self._duckdb_context is not None

        schema = (
            self._duckdb_context.client.sql("DESCRIBE refined_traces_fees_v1")
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
            "block_number": "BIGINT",
            "block_timestamp": "UINTEGER",
            "transaction_hash": "VARCHAR",
            "transaction_index": "BIGINT",
            "trace_from_address": "VARCHAR",
            "trace_to_address": "VARCHAR",
            "trace_gas_limit": "BIGINT",
            "trace_gas_used": "BIGINT",
            "trace_address": "VARCHAR",
            "trace_type": "VARCHAR",
            "call_type": "VARCHAR",
            "error": "VARCHAR",
            "trace_method_id": "VARCHAR",
            "trace_success": "BOOLEAN",
            "trace_depth": "BIGINT",
            "parent_trace_address": "VARCHAR",
            "num_traces_in_txn": "BIGINT",
            "sum_subtraces_gas_used": "DECIMAL(38,0)",
            "gas_used_minus_subtraces": "DECIMAL(38,0)",
            "tx_l2_fee_native_minus_subtraces": "DECIMAL(38,20)",
            "tx_l2_base_fee_native_minus_subtraces": "DECIMAL(38,20)",
            "tx_l2_priority_fee_native_minus_subtraces": "DECIMAL(38,20)",
            "tx_l2_legacy_base_fee_native_minus_subtraces": "DECIMAL(38,20)",
            "tx_l2_gas_used_amortized_by_call": "DOUBLE",
            "tx_l1_gas_used_unified_amortized_by_call": "DOUBLE",
            "tx_l1_base_scaled_size_amortized_by_call": "DOUBLE",
            "tx_l1_blob_scaled_size_amortized_by_call": "DOUBLE",
            "tx_estimated_size_amortized_by_call": "DOUBLE",
            "tx_fee_native_amortized_by_call": "DOUBLE",
            "tx_l2_fee_native_amortized_by_call": "DOUBLE",
            "tx_l1_fee_native_amortized_by_call": "DOUBLE",
            "tx_l2_base_fee_native_amortized_by_call": "DOUBLE",
            "tx_l2_priority_fee_native_amortized_by_call": "DOUBLE",
        }

    def test_single_tx(self):
        """Look at the transformation results for a single transaction."""

        assert self._duckdb_context is not None

        # ---------------------------
        # A transaction with 3 traces
        # ---------------------------
        tx_hash = "0x24efe37e2759838fead0bc911b6a25e3f575716ebbcd002c2e8c89c5bb7bd894"

        raw_transaction = (
            self._duckdb_context.client.sql(f"""
        SELECT receipt_gas_used
        FROM ingestion_transactions_v1 WHERE hash = '{tx_hash}'
        """)
            .pl()
            .to_dicts()
        )
        assert raw_transaction == [
            {"receipt_gas_used": 189126},
        ]

        raw_traces = (
            self._duckdb_context.client.sql(f"""
        SELECT trace_type, call_type, trace_address, gas_used
        FROM ingestion_traces_v1 WHERE transaction_hash = '{tx_hash}'
        """)
            .pl()
            .to_dicts()
        )
        assert raw_traces == [
            {
                "trace_type": "call",
                "call_type": "call",
                "trace_address": "",
                "gas_used": 189126,
            },
            {
                "trace_type": "call",
                "call_type": "delegatecall",
                "trace_address": "0",
                "gas_used": 159392,
            },
            {
                "trace_type": "call",
                "call_type": "staticcall",
                "trace_address": "0,0",
                "gas_used": 3000,
            },
        ]

        subtraces_refined = (
            self._duckdb_context.client.sql(f"""
            SELECT 
                trace_address,
                trace_depth,
                parent_trace_address,
                num_traces_in_txn,
                sum_subtraces_gas_used,
                gas_used_minus_subtraces,
                tx_l2_gas_used_amortized_by_call
            FROM refined_traces_fees_v1 WHERE transaction_hash = '{tx_hash}'
            """)
            .pl()
            .to_dicts()
        )

        assert subtraces_refined == [
            {
                "trace_address": "",
                "trace_depth": 0,
                "parent_trace_address": "none",
                "num_traces_in_txn": 3,
                "sum_subtraces_gas_used": Decimal("159392"),
                "gas_used_minus_subtraces": Decimal("29734"),
                "tx_l2_gas_used_amortized_by_call": 63042.0,
            },
            {
                "trace_address": "0",
                "trace_depth": 1,
                "parent_trace_address": "",
                "num_traces_in_txn": 3,
                "sum_subtraces_gas_used": Decimal("3000"),
                "gas_used_minus_subtraces": Decimal("156392"),
                "tx_l2_gas_used_amortized_by_call": 63042.0,
            },
            {
                "trace_address": "0,0",
                "trace_depth": 2,
                "parent_trace_address": "0",
                "num_traces_in_txn": 3,
                "sum_subtraces_gas_used": Decimal("0"),
                "gas_used_minus_subtraces": Decimal("3000"),
                "tx_l2_gas_used_amortized_by_call": 63042.0,
            },
        ]
