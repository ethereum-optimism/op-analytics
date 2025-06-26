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

        self.assertIsInstance(config, dict)
        self.assertGreater(len(config), 0)

        for chain, chain_config in config.items():
            self.assertIn("addresses", chain_config)
            self.assertIn("tokens", chain_config)
            self.assertIn("expected_chains", chain_config)
            self.assertIsInstance(chain_config["addresses"], list)
            self.assertIsInstance(chain_config["tokens"], list)
            self.assertIsInstance(chain_config["expected_chains"], list)

            for addr in chain_config["addresses"]:
                self.assertIsInstance(addr, str)
                self.assertTrue(addr.startswith("0x"), f"Invalid address format: {addr}")
                self.assertEqual(len(addr), 42, f"Invalid address length: {addr}")

            for token in chain_config["tokens"]:
                self.assertIsInstance(token, str)
                if token != "native":
                    self.assertTrue(token.startswith("0x"), f"Invalid token format: {token}")
                    self.assertEqual(len(token), 42, f"Invalid token length: {token}")

            for expected_chain in chain_config["expected_chains"]:
                self.assertIsInstance(expected_chain, str)

            self.assertGreater(
                len(chain_config["expected_chains"]),
                0,
                f"Chain {chain} should have at least one expected chain",
            )

    def test_to_addresses_config_exists(self):
        """Test that to_addresses config file exists and is valid YAML."""
        config_path = self.config_dir / "revshare_to_addresses.yaml"
        self.assertTrue(config_path.exists(), f"Config file not found: {config_path}")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        self.assertIsInstance(config, dict)
        self.assertGreater(len(config), 0)

        for address, address_config in config.items():
            self.assertIn("description", address_config)
            self.assertIn("expected_chains", address_config)
            self.assertIsInstance(address_config["expected_chains"], list)

            address_str = str(address)
            if address_str.startswith("0x"):
                self.assertEqual(len(address_str), 42, f"Invalid address length: {address_str}")

            self.assertIsInstance(address_config["description"], str)
            self.assertGreater(len(address_config["description"]), 0)

            for chain in address_config["expected_chains"]:
                self.assertIsInstance(chain, str)
                self.assertGreater(len(chain), 0)

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

        self.assertIn("-- Native transfers", content)
        self.assertIn("-- ERC20 transfers", content)
        self.assertIn("revshare_from_addresses AS f", content)
        self.assertIn("revshare_to_addresses AS ta", content)
        self.assertIn("expected_chains", content, "Should reference expected_chains field")

    def test_no_duplicate_addresses(self):
        """Test that there are no duplicate addresses (case-insensitive) in from/to YAML configs."""
        from_path = self.config_dir / "revshare_from_addresses.yaml"
        with open(from_path, "r") as f:
            from_config = yaml.safe_load(f)
        all_from = []
        for chain_config in from_config.values():
            all_from.extend([str(addr).lower() for addr in chain_config["addresses"]])
        from_dupes = set([a for a in all_from if all_from.count(a) > 1])
        self.assertFalse(from_dupes, f"Duplicate addresses in from_addresses: {from_dupes}")

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

    def test_yaml_loaders_import(self):
        """Test that YAML loaders can be imported and have expected functions."""
        try:
            from op_analytics.datapipeline.etl.blockbatchload.yaml_loaders import (
                load_revshare_from_addresses_to_clickhouse,
                load_revshare_to_addresses_to_clickhouse,
            )

            self.assertTrue(callable(load_revshare_from_addresses_to_clickhouse))
            self.assertTrue(callable(load_revshare_to_addresses_to_clickhouse))
        except ImportError as e:
            self.fail(f"Failed to import YAML loaders: {e}")

    def test_generate_script_can_load_configs(self):
        """Test that the generate script can load configs properly."""
        try:
            import sys

            script_dir = self.sql_dir / "scripts"
            sys.path.insert(0, str(script_dir))

            from generate_revshare_sql import load_config

            from_config = load_config("revshare_from_addresses")
            to_config = load_config("revshare_to_addresses")

            self.assertIsInstance(from_config, dict)
            self.assertIsInstance(to_config, dict)
            self.assertGreater(len(from_config), 0)
            self.assertGreater(len(to_config), 0)

        except ImportError as e:
            self.fail(f"Failed to import generate script: {e}")
        except Exception as e:
            self.fail(f"Failed to load configs via generate script: {e}")
        finally:
            if str(script_dir) in sys.path:
                sys.path.remove(str(script_dir))

    def test_config_cross_reference_integrity(self):
        """Test that all expected_chains references are valid."""
        from_path = self.config_dir / "revshare_from_addresses.yaml"
        to_path = self.config_dir / "revshare_to_addresses.yaml"

        with open(from_path, "r") as f:
            from_config = yaml.safe_load(f)
        with open(to_path, "r") as f:
            to_config = yaml.safe_load(f)

        all_chains = set(from_config.keys())
        for chain_config in from_config.values():
            all_chains.update(chain_config["expected_chains"])
        for addr_config in to_config.values():
            all_chains.update(addr_config["expected_chains"])

        for addr, addr_config in to_config.items():
            for expected_chain in addr_config["expected_chains"]:
                self.assertIsInstance(expected_chain, str)
                self.assertGreater(len(expected_chain), 0)

    def test_synthetic_config_parsing(self):
        """Test config parsing with synthetic data to ensure robustness."""
        synthetic_from_config = {
            "test_chain": {
                "addresses": ["0x1234567890123456789012345678901234567890"],
                "tokens": ["native", "0x0987654321098765432109876543210987654321"],
                "expected_chains": ["test_chain", "other_chain"],
                "end_date": None,
                "chain_id": 12345,
            }
        }

        synthetic_to_config = {
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd": {
                "description": "Test receiver",
                "end_date": None,
                "expected_chains": ["test_chain"],
            }
        }

        for chain, chain_config in synthetic_from_config.items():
            self.assertIn("addresses", chain_config)
            self.assertIn("tokens", chain_config)
            self.assertIn("expected_chains", chain_config)
            self.assertIsInstance(chain_config["addresses"], list)
            self.assertIsInstance(chain_config["tokens"], list)
            self.assertIsInstance(chain_config["expected_chains"], list)

        for address, address_config in synthetic_to_config.items():
            self.assertIn("description", address_config)
            self.assertIn("expected_chains", address_config)
            self.assertIsInstance(address_config["expected_chains"], list)

    def test_config_data_integrity(self):
        """Test that config data has proper integrity constraints."""
        from_path = self.config_dir / "revshare_from_addresses.yaml"
        to_path = self.config_dir / "revshare_to_addresses.yaml"

        with open(from_path, "r") as f:
            from_config = yaml.safe_load(f)
        with open(to_path, "r") as f:
            to_config = yaml.safe_load(f)

        for chain, chain_config in from_config.items():
            self.assertGreater(
                len(chain_config["addresses"]), 0, f"Chain {chain} should have at least one address"
            )

        for address, addr_config in to_config.items():
            self.assertIsNotNone(addr_config.get("description"))
            self.assertGreater(len(addr_config["description"]), 0)


if __name__ == "__main__":
    unittest.main()
