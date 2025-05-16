import unittest
from unittest.mock import patch

from op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date import ClickHouseDateETL
from op_analytics.datapipeline.etl.blockbatchloaddaily.readers import (
    DateChainBatch,
    ALL_CHAINS_SENTINEL,
)


class TestClickHouseDateETL(unittest.TestCase):
    def _create_etl_instance(
        self,
        output_root_path="test_db/test_table",
        inputs_blockbatch=None,
        inputs_clickhouse=None,
        ignore_zero_rows_chains=None,
        ignore_zero_rows_chain_dts=None,
    ):
        return ClickHouseDateETL(
            output_root_path=output_root_path,
            inputs_blockbatch=inputs_blockbatch or [],
            inputs_clickhouse=inputs_clickhouse or [],
            ignore_zero_rows_chains=ignore_zero_rows_chains or [],
            ignore_zero_rows_chain_dts=ignore_zero_rows_chain_dts or [],
        )

    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.goldsky_mainnet_chains")
    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.construct_batches")
    @patch.object(ClickHouseDateETL, "existing_markers")
    def test_pending_batches_all_chains_present(
        self,
        mock_existing_markers,
        mock_construct_batches,
        mock_goldsky_mainnet_chains,
    ):
        # Setup mocks
        mock_goldsky_mainnet_chains.return_value = ["chain_a", "chain_b"]
        mock_construct_batches.return_value = [
            DateChainBatch(dt="2023-01-01", chain="chain_a"),
            DateChainBatch(dt="2023-01-01", chain="chain_b"),
            DateChainBatch(dt="2023-01-02", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_b"),
        ]
        mock_existing_markers.return_value = []

        etl_instance = self._create_etl_instance()
        pending = etl_instance.pending_batches(range_spec="2023-01-01_2023-01-02")

        expected_pending = [
            DateChainBatch(dt="2023-01-01", chain=ALL_CHAINS_SENTINEL),
            DateChainBatch(dt="2023-01-02", chain=ALL_CHAINS_SENTINEL),
        ]
        self.assertEqual(sorted(pending), sorted(expected_pending))
        mock_existing_markers.assert_called_once_with(
            range_spec="2023-01-01_2023-01-02", chains=[ALL_CHAINS_SENTINEL]
        )

    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.goldsky_mainnet_chains")
    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.construct_batches")
    @patch.object(ClickHouseDateETL, "existing_markers")
    def test_pending_batches_with_ignore_zero_rows_chains(
        self,
        mock_existing_markers,
        mock_construct_batches,
        mock_goldsky_mainnet_chains,
    ):
        # Setup mocks
        mock_goldsky_mainnet_chains.return_value = ["chain_a", "chain_b", "chain_c"]
        # chain_b is missing for 2023-01-01, but it's ignored
        mock_construct_batches.return_value = [
            DateChainBatch(dt="2023-01-01", chain="chain_a"),
            DateChainBatch(dt="2023-01-01", chain="chain_c"),
            # chain_c is missing for 2023-01-02, not ignored
            DateChainBatch(dt="2023-01-02", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_b"),
        ]
        mock_existing_markers.return_value = []

        etl_instance = self._create_etl_instance(ignore_zero_rows_chains=["chain_b"])
        pending = etl_instance.pending_batches(range_spec="2023-01-01_2023-01-02")

        expected_pending = [
            DateChainBatch(dt="2023-01-01", chain=ALL_CHAINS_SENTINEL),
            # 2023-01-02 should not be pending as chain_c is missing and not ignored
        ]
        self.assertEqual(sorted(pending), sorted(expected_pending))

    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.goldsky_mainnet_chains")
    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.construct_batches")
    @patch.object(ClickHouseDateETL, "existing_markers")
    def test_pending_batches_with_ignore_zero_rows_chain_dts(
        self,
        mock_existing_markers,
        mock_construct_batches,
        mock_goldsky_mainnet_chains,
    ):
        # Setup mocks
        mock_goldsky_mainnet_chains.return_value = ["chain_a", "chain_b", "chain_c"]
        # chain_b is missing for 2023-01-01, but ('chain_b', '2023-01-01') is ignored
        # chain_c is missing for 2023-01-02, but ('chain_c', '2023-01-02') is NOT ignored
        mock_construct_batches.return_value = [
            DateChainBatch(dt="2023-01-01", chain="chain_a"),
            DateChainBatch(dt="2023-01-01", chain="chain_c"),
            DateChainBatch(dt="2023-01-02", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_b"),
            # chain_b is missing for 2023-01-03, and ('chain_b', '2023-01-03') is not ignored
            DateChainBatch(dt="2023-01-03", chain="chain_a"),
            DateChainBatch(dt="2023-01-03", chain="chain_c"),
        ]
        mock_existing_markers.return_value = []

        etl_instance = self._create_etl_instance(
            ignore_zero_rows_chain_dts=[("chain_b", "2023-01-01")]
        )
        pending = etl_instance.pending_batches(range_spec="2023-01-01_2023-01-03")

        expected_pending = [
            DateChainBatch(dt="2023-01-01", chain=ALL_CHAINS_SENTINEL),
            # 2023-01-02 should not be pending (chain_c missing)
            # 2023-01-03 should not be pending (chain_b missing)
        ]
        self.assertEqual(sorted(pending), sorted(expected_pending))

    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.goldsky_mainnet_chains")
    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.construct_batches")
    @patch.object(ClickHouseDateETL, "existing_markers")
    def test_pending_batches_no_ignore_missing_chain(
        self,
        mock_existing_markers,
        mock_construct_batches,
        mock_goldsky_mainnet_chains,
    ):
        # Setup mocks
        mock_goldsky_mainnet_chains.return_value = ["chain_a", "chain_b"]
        # chain_b is missing for 2023-01-01
        mock_construct_batches.return_value = [
            DateChainBatch(dt="2023-01-01", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_b"),
        ]
        mock_existing_markers.return_value = []

        etl_instance = self._create_etl_instance()  # No ignores
        pending = etl_instance.pending_batches(range_spec="2023-01-01_2023-01-02")

        expected_pending = [
            # 2023-01-01 should not be pending (chain_b missing)
            DateChainBatch(dt="2023-01-02", chain=ALL_CHAINS_SENTINEL),
        ]
        self.assertEqual(sorted(pending), sorted(expected_pending))

    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.goldsky_mainnet_chains")
    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.construct_batches")
    @patch.object(ClickHouseDateETL, "existing_markers")
    def test_pending_batches_with_existing_markers(
        self,
        mock_existing_markers,
        mock_construct_batches,
        mock_goldsky_mainnet_chains,
    ):
        # Setup mocks
        mock_goldsky_mainnet_chains.return_value = ["chain_a", "chain_b"]
        mock_construct_batches.return_value = [
            DateChainBatch(dt="2023-01-01", chain="chain_a"),
            DateChainBatch(dt="2023-01-01", chain="chain_b"),
            DateChainBatch(dt="2023-01-02", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_b"),
            DateChainBatch(dt="2023-01-03", chain="chain_a"),
            DateChainBatch(dt="2023-01-03", chain="chain_b"),
        ]
        # 2023-01-01 is already processed
        mock_existing_markers.return_value = [
            DateChainBatch(dt="2023-01-01", chain=ALL_CHAINS_SENTINEL)
        ]

        etl_instance = self._create_etl_instance()
        pending = etl_instance.pending_batches(range_spec="2023-01-01_2023-01-03")

        expected_pending = [
            DateChainBatch(dt="2023-01-02", chain=ALL_CHAINS_SENTINEL),
            DateChainBatch(dt="2023-01-03", chain=ALL_CHAINS_SENTINEL),
        ]
        self.assertEqual(sorted(pending), sorted(expected_pending))
        mock_existing_markers.assert_called_once_with(
            range_spec="2023-01-01_2023-01-03", chains=[ALL_CHAINS_SENTINEL]
        )

    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.goldsky_mainnet_chains")
    @patch("op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec_date.construct_batches")
    @patch.object(ClickHouseDateETL, "existing_markers")
    def test_pending_batches_ignore_chain_and_chain_dt_interaction(
        self,
        mock_existing_markers,
        mock_construct_batches,
        mock_goldsky_mainnet_chains,
    ):
        # Setup mocks
        mock_goldsky_mainnet_chains.return_value = ["chain_a", "chain_b", "chain_c", "chain_d"]
        mock_construct_batches.return_value = [
            # Date 2023-01-01: chain_b missing (ignored by ignore_zero_rows_chains)
            # chain_c missing (ignored by ignore_zero_rows_chain_dts)
            DateChainBatch(dt="2023-01-01", chain="chain_a"),
            DateChainBatch(dt="2023-01-01", chain="chain_d"),
            # Date 2023-01-02: chain_b missing (ignored by ignore_zero_rows_chains)
            # chain_c missing (NOT ignored, as ignore_zero_rows_chain_dts is specific to 2023-01-01)
            DateChainBatch(dt="2023-01-02", chain="chain_a"),
            DateChainBatch(dt="2023-01-02", chain="chain_d"),
            # Date 2023-01-03: All required chains present (chain_b ignored)
            DateChainBatch(dt="2023-01-03", chain="chain_a"),
            DateChainBatch(dt="2023-01-03", chain="chain_c"),
            DateChainBatch(dt="2023-01-03", chain="chain_d"),
        ]
        mock_existing_markers.return_value = []

        etl_instance = self._create_etl_instance(
            ignore_zero_rows_chains=["chain_b"],
            ignore_zero_rows_chain_dts=[("chain_c", "2023-01-01")],
        )
        pending = etl_instance.pending_batches(range_spec="2023-01-01_2023-01-03")

        expected_pending = [
            DateChainBatch(
                dt="2023-01-01", chain=ALL_CHAINS_SENTINEL
            ),  # chain_b and chain_c ignored for this date
            # 2023-01-02: chain_c is missing and not ignored for this date
            DateChainBatch(dt="2023-01-03", chain=ALL_CHAINS_SENTINEL),  # chain_b ignored
        ]
        self.assertEqual(sorted(pending), sorted(expected_pending))


if __name__ == "__main__":
    unittest.main()
