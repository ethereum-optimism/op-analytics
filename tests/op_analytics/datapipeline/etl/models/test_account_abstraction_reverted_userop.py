from datetime import date
from pathlib import Path

from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestAccountAbstractionRevertBeforeInnerHandleOp(ModelTestBase):
    """User operation reverts before calling ``innerHandleOp``.

    The log is emitted but the corresponding ``innerHandleOp`` trace is missing.
    This scenario should not trigger the data-quality check introduced in
    ``data_quality_check_01``.
    """

    model = "account_abstraction"
    inputdata = InputTestData.at(__file__)
    chains = ["base"]
    target_range = date(2024, 9, 17)
    block_filters = [
        "{block_number} IN (19910194) OR block_number % 100 < 1",
    ]

    _enable_fetching = False

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        assert cls._duckdb_context is not None
        # Insert a reverted user operation log with no matching trace.
        cls._duckdb_context.client.sql(
            """
            INSERT INTO account_abstraction__useroperationevent_logs (
                dt, chain, chain_id, network,
                block_timestamp, block_number, block_hash,
                transaction_hash, transaction_index, log_index,
                contract_address, userophash, sender, paymaster, nonce,
                success, actualGasCost, actualGasUsed
            )
            VALUES (
                DATE '2024-01-01', 'base', 8453, 'mainnet',
                0, 1, '0x0',
                '0x58667461e08c02057cae4a8e21edd8fbc41b1385a8321ffb3dbe0d609b6fff9b',
                0, 0,
                '0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789',
                '0x1111111111111111111111111111111111111111111111111111111111111111',
                '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
                '0x0000000000000000000000000000000000000000',
                '0', False, '0', '0'
            );
            """
        )

    def test_data_quality_allows_reverted_userop(self):
        assert self._duckdb_context is not None
        # Execute the relaxed data-quality check.
        dq_query_path = Path(
            "src/op_analytics/datapipeline/models/templates/account_abstraction/"
            "data_quality_check_01.sql.j2"
        )
        dq_query = dq_query_path.read_text()
        errors = (
            self._duckdb_context.client.sql(dq_query)
            .pl()
            .to_dicts()
        )
        assert errors == []

        # Document expected behaviour for future maintainers.
        logs = (
            self._duckdb_context.client.sql(
                """
                SELECT transaction_hash, success
                FROM account_abstraction__useroperationevent_logs
                WHERE transaction_hash = '0x58667461e08c02057cae4a8e21edd8fbc41b1385a8321ffb3dbe0d609b6fff9b'
                """
            )
            .pl()
            .to_dicts()
        )
        assert logs == [
            {
                "transaction_hash": "0x58667461e08c02057cae4a8e21edd8fbc41b1385a8321ffb3dbe0d609b6fff9b",
                "success": False,
            }
        ]

        traces = (
            self._duckdb_context.client.sql(
                """
                SELECT transaction_hash
                FROM account_abstraction__enriched_entrypoint_traces
                WHERE transaction_hash = '0x58667461e08c02057cae4a8e21edd8fbc41b1385a8321ffb3dbe0d609b6fff9b'
                """
            )
            .pl()
            .to_dicts()
        )
        assert traces == []
