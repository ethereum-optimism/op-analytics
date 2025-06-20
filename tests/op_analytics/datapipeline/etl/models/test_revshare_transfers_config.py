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
        self.config_dir = repo_root / "src/op_analytics/configs"
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

        # Check for key components in the new simplified SQL
        self.assertIn("-- Native transfers", content)
        self.assertIn("-- ERC20 transfers", content)
        self.assertIn("revshare_from_addresses AS f", content)
        self.assertIn("revshare_to_addresses AS ta", content)
        self.assertIn("hasAny(f.tokens, [lower(t.contract_address)])", content)

    def test_no_duplicate_addresses(self):
        """Test that there are no duplicate addresses (case-insensitive) in from/to YAML configs."""
        # Check from_addresses
        from_path = self.config_dir / "revshare_from_addresses.yaml"
        with open(from_path, "r") as f:
            from_config = yaml.safe_load(f)
        all_from = []
        for chain_config in from_config.values():
            all_from.extend([str(addr).lower() for addr in chain_config["addresses"]])
        from_dupes = set([a for a in all_from if all_from.count(a) > 1])
        self.assertFalse(from_dupes, f"Duplicate addresses in from_addresses: {from_dupes}")

        # Check to_addresses
        to_path = self.config_dir / "revshare_to_addresses.yaml"
        with open(to_path, "r") as f:
            to_config = yaml.safe_load(f)
        all_to = [str(addr).lower() for addr in to_config.keys()]
        to_dupes = set([a for a in all_to if all_to.count(a) > 1])
        self.assertFalse(to_dupes, f"Duplicate addresses in to_addresses: {to_dupes}")

    def test_no_duplicate_tokens(self):
        """Test that there are no duplicate tokens within each address configuration."""
        from_path = self.config_dir / "revshare_from_addresses.yaml"
        with open(from_path, "r") as f:
            from_config = yaml.safe_load(f)

        for chain_name, chain_config in from_config.items():
            tokens = [str(token).lower() for token in chain_config["tokens"]]
            token_dupes = set([t for t in tokens if tokens.count(t) > 1])
            self.assertFalse(token_dupes, f"Duplicate tokens in {chain_name}: {token_dupes}")


if __name__ == "__main__":
    unittest.main()
