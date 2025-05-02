from op_analytics.coreutils.testutils.inputdata import InputTestData
from op_analytics.datapipeline.models.compute.testutils import ModelTestBase


class TestRefinedTraces002(ModelTestBase):
    model = "refined_traces"
    inputdata = InputTestData.at(__file__)
    chains = ["fraxtal"]
    target_range = 19572001
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
                SELECT 'traces' as name, COUNT(*) as total FROM refined_traces_fees_v2
                
                UNION ALL
                
                SELECT 'txs' as name, COUNT(*) as total FROM refined_transactions_fees_v2
                
                """
            )
            .pl()
            .to_dicts()
        )

        assert row_counts == [
            {"name": "traces", "total": 1988},
            {"name": "txs", "total": 62},
        ]
