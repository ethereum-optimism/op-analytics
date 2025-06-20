"""
Test for revshare_transfers configuration and SQL generation.
"""

import unittest
import yaml
from pathlib import Path


class TestRevshareTransfersConfig(unittest.TestCase):
    """Test revshare_transfers configuration files and SQL generation."""

    def setUp(self):
        """Set up test paths."""
        repo_root = Path(__file__).resolve().parents[5]
        self.config_dir = repo_root / "src/op_analytics/datapipeline/models/config"
        self.sql_dir = (
            repo_root / "src/op_analytics/datapipeline/etl/blockbatchload/ddl/revshare_transfers"
        )

    def test_from_addresses_config_exists(self):
        """Test that from_addresses config file exists and is valid YAML."""
        config_path = self.config_dir / "revshare_from_addresses.yaml"
        self.assertTrue(config_path.exists(), f"Config file not found: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Basic validation
        self.assertIsInstance(config, dict)
        self.assertGreater(len(config), 0)

        # Check each chain has required fields
        for chain, chain_config in config.items():
            self.assertIn("addresses", chain_config)
            self.assertIn("tokens", chain_config)
            self.assertIn("expected_chains", chain_config)
            self.assertIsInstance(chain_config["addresses"], list)
            self.assertIsInstance(chain_config["tokens"], list)
            self.assertIsInstance(chain_config["expected_chains"], list)

    def test_to_addresses_config_exists(self):
        """Test that to_addresses config file exists and is valid YAML."""
        config_path = self.config_dir / "revshare_to_addresses.yaml"
        self.assertTrue(config_path.exists(), f"Config file not found: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Basic validation
        self.assertIsInstance(config, dict)
        self.assertGreater(len(config), 0)

        # Check each address has required fields
        for address, address_config in config.items():
            self.assertIn("description", address_config)
            self.assertIn("expected_chains", address_config)
            self.assertIsInstance(address_config["expected_chains"], list)

    def test_sql_files_exist(self):
        """Test that SQL files exist."""
        create_sql = self.sql_dir / "revshare_transfers_v1__CREATE.sql"
        insert_sql = self.sql_dir / "revshare_transfers_v1__INSERT.sql"

        self.assertTrue(create_sql.exists(), f"CREATE SQL file not found: {create_sql}")
        self.assertTrue(insert_sql.exists(), f"INSERT SQL file not found: {insert_sql}")

    def test_generation_script_exists(self):
        """Test that the generation script exists."""
        script_path = self.sql_dir / "scripts" / "generate_revshare_sql.py"
        self.assertTrue(script_path.exists(), f"Generation script not found: {script_path}")

    def test_sql_contains_expected_content(self):
        """Test that the INSERT SQL contains expected content."""
        insert_sql = self.sql_dir / "revshare_transfers_v1__INSERT.sql"

        with open(insert_sql, "r") as f:
            content = f.read()

        # Check for key components
        self.assertIn("WITH from_addresses AS (", content)
        self.assertIn("to_addresses AS (", content)
        self.assertIn("-- Native transfers", content)
        self.assertIn("-- ERC20 transfers", content)
        self.assertIn("hasAny(f.revshare_from_addresses, [t.from_address])", content)


if __name__ == "__main__":
    unittest.main()
