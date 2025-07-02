"""
Integration tests for revshare transfers blockbatchload pipeline.
Tests the YAML loaders and dataset configuration.
"""

import unittest
import yaml
from pathlib import Path
from op_analytics.coreutils.path import repo_path

from op_analytics.datapipeline.etl.blockbatchload.yaml_loaders import (
    load_revshare_from_addresses_to_clickhouse,
    load_revshare_to_addresses_to_clickhouse,
)
from op_analytics.datapipeline.etl.blockbatchload.datasets import REVSHARE_TRANSFERS
from op_analytics.datapipeline.etl.blockbatchload.compute.testutils import BlockBatchTestBase
from op_analytics.coreutils.testutils.inputdata import InputTestData


class TestRevshareTransfersIntegration(unittest.TestCase):
    """Integration tests for revshare transfers blockbatchload pipeline."""

    def test_yaml_loaders_are_callable(self):
        """Test that YAML loader functions are callable."""
        self.assertTrue(callable(load_revshare_from_addresses_to_clickhouse))
        self.assertTrue(callable(load_revshare_to_addresses_to_clickhouse))

    def test_revshare_transfers_dataset_config(self):
        """Test that REVSHARE_TRANSFERS dataset is properly configured."""
        self.assertIsNotNone(REVSHARE_TRANSFERS)
        self.assertEqual(
            REVSHARE_TRANSFERS.input_root_paths,
            [
                "blockbatch/token_transfers/erc20_transfers_v1",
                "blockbatch/native_transfers/native_transfers_v1",
            ],
        )
        self.assertEqual(
            REVSHARE_TRANSFERS.output_root_path,
            "blockbatch/revshare_transfers/revshare_transfers_v1",
        )
        self.assertFalse(REVSHARE_TRANSFERS.enforce_non_zero_row_count)

    def test_dagster_assets_importable(self):
        """Test that Dagster assets can be imported and are callable."""
        try:
            from op_analytics.dagster.assets.blockbatchload import (
                revshare_from_addresses,
                revshare_to_addresses,
                revshare_transfers,
            )

            self.assertTrue(callable(revshare_from_addresses))
            self.assertTrue(callable(revshare_to_addresses))
            self.assertTrue(callable(revshare_transfers))

        except ImportError as e:
            self.fail(f"Failed to import Dagster assets: {e}")

    def test_config_files_exist_and_valid(self):
        """Test that actual config files are valid and contain expected data."""
        from_path = repo_path("src/op_analytics/configs/revshare_from_addresses.yaml")
        to_path = repo_path("src/op_analytics/configs/revshare_to_addresses.yaml")

        # Test from addresses config
        with open(from_path, "r") as f:
            from_config = yaml.safe_load(f)

        self.assertIsInstance(from_config, dict)
        self.assertGreater(len(from_config), 0)

        # Test to addresses config
        with open(to_path, "r") as f:
            to_config = yaml.safe_load(f)

        self.assertIsInstance(to_config, dict)
        self.assertGreater(len(to_config), 0)

        # Verify at least one chain has addresses
        has_addresses = False
        for chain_config in from_config.values():
            if chain_config.get("addresses") and len(chain_config["addresses"]) > 0:
                has_addresses = True
                break

        self.assertTrue(has_addresses, "At least one chain should have addresses configured")

    def test_sql_files_exist_and_valid(self):
        """Test that SQL files exist and contain expected content."""
        sql_dir = repo_path(
            "src/op_analytics/datapipeline/etl/blockbatchload/ddl/revshare_transfers"
        )

        create_sql = Path(sql_dir) / "revshare_transfers_v1__CREATE.sql"
        insert_sql = Path(sql_dir) / "revshare_transfers_v1__INSERT.sql"

        self.assertTrue(create_sql.exists(), f"CREATE SQL file not found: {create_sql}")
        self.assertTrue(insert_sql.exists(), f"INSERT SQL file not found: {insert_sql}")

        # Test that SQL files contain expected content
        with open(insert_sql, "r") as f:
            content = f.read()

        self.assertIn("revshare_from_addresses", content)
        self.assertIn("revshare_to_addresses", content)
        self.assertIn("erc20_transfers_", content)
        self.assertIn("native_transfers_", content)


class TestRevshareTransfersBlockBatch(BlockBatchTestBase):
    dataset = REVSHARE_TRANSFERS
    chains = ["base"]
    target_range = "@20250622:20250623"
    block_filters = [
        "{block_number} IN (31880531)",
    ]
    inputdata = InputTestData.at(__file__)
    _enable_fetching = False  # Set to True only when fetching test data for the first time

    def test_specific_base_transfer(self):
        result = self.query_output_table(
            """
            SELECT block_number, transaction_hash, value, from_address, to_address, token_address
            FROM revshare_transfers_v1
            WHERE block_number = 31880531
              AND transaction_hash = '0x1b85d1be582a90c2ae9682e0e1adf72f29e3aed609bbc57714f5676493716162'
              AND chain = 'base'
            """
        )
        self.assertGreater(
            len(result),
            0,
            "No revshare transfer found for block 31880531, tx 0x1b85d1be582a90c2ae9682e0e1adf72f29e3aed609bbc57714f5676493716162",
        )
        self.assertEqual(str(result[0]["value"]), "72212747869383783996")
        print(
            f"Found transfer: block={result[0]['block_number']}, tx={result[0]['transaction_hash']}, value={result[0]['value']}, from={result[0]['from_address']}, to={result[0]['to_address']}, token={result[0]['token_address']}"
        )


if __name__ == "__main__":
    unittest.main()
