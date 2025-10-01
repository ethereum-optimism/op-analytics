"""Tests for superchain config data pipeline."""

import pytest
from unittest.mock import Mock, patch
import polars as pl

from op_analytics.datasources.chainsmeta.superchain.superchainconfig import (
    execute_pull,
    pull_superchain_registry,
    fetch_chain_config,
    get_available_chains,
    SUPERCHAIN_REGISTRY_SCHEMA,
)


class TestSuperchainConfig:
    """Test superchain config data pipeline."""

    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.new_session")
    def test_get_available_chains_success(self, mock_session):
        """Test getting list of available chains dynamically."""
        # Mock successful GitHub API response
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance

        mock_response = Mock()
        mock_response.json.return_value = [
            {"type": "file", "name": "op.toml"},
            {"type": "file", "name": "base.toml"},
            {"type": "file", "name": "ink.toml"},
            {"type": "file", "name": "world.toml"},
            {"type": "file", "name": "README.md"},  # Should be ignored
        ]
        mock_response.raise_for_status.return_value = None
        mock_session_instance.get.return_value = mock_response

        chains = get_available_chains()

        assert isinstance(chains, list)
        assert len(chains) == 4
        assert "op" in chains
        assert "base" in chains
        assert "ink" in chains
        assert "world" in chains
        assert chains == ["base", "ink", "op", "world"]  # Should be sorted

    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.new_session")
    def test_get_available_chains_api_failure(self, mock_session):
        """Test that GitHub API failure raises an exception."""
        # Mock GitHub API failure
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.get.side_effect = Exception("API Error")

        with pytest.raises(ValueError, match="Cannot fetch chain list from GitHub API"):
            get_available_chains()

    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.new_session")
    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.ChainsMeta")
    def test_fetch_chain_config_success(self, mock_chains_meta, mock_session):
        """Test successful chain config fetch."""
        # Mock session and response
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance

        mock_response = Mock()
        mock_response.text = """
name = "op"
chain_id = 10
public_rpc = "https://mainnet.optimism.io"
sequencer_rpc = "https://mainnet.optimism.io"
explorer = "https://optimistic.etherscan.io"
superchain_level = 1
governed_by_optimism = true
superchain_time = 1736445601
data_availability_type = "eth-da"
batch_inbox_addr = "0x00f9BCEe08DCe4F0e7906c1f6cFb10c77802EEd0"
block_time = 2
seq_window_size = 3600
max_sequencer_drift = 600

[hardforks]
canyon_time = 0
delta_time = 0
ecotone_time = 0
fjord_time = 0
granite_time = 0
holocene_time = 1736445601
isthmus_time = 1746806401

[optimism]
eip1559_elasticity = 3
eip1559_denominator = 2
eip1559_denominator_canyon = 2

[genesis]
l2_time = 1731366083

[genesis.l1]
hash = "0x5e7db5c04973dd6e06ac7e2abf5b9373089f0f09e8ae8231642f0c6aff177e27"
number = 21167590

[genesis.l2]
hash = "0xbe7112a730b1fae8d94115271adc600559ebe87c75df1d2df9414bd7298eb7fb"
number = 0

[genesis.system_config]
batcherAddress = "0x2b8733E8c60A928b19BB7db1D79b918e8E09AC8c"
overhead = "0x0000000000000000000000000000000000000000000000000000000000000000"
scalar = "0x010000000000000000000000000000000000000000000000000c3a30000060a4"
gasLimit = 30000000

[roles]
ProxyAdminOwner = "0x5a0Aae59D09fccBdDb6C6CcEB07B7279367C3d2A"

[addresses]
L1StandardBridgeProxy = "0x564Eb0CeFCcA86160649a8986C419693c82F3678"
OptimismPortalProxy = "0xB20f99b598E8d888d1887715439851BC68806b22"
SystemConfigProxy = "0x34A564BbD863C4bf73Eca711Cf38a77C4Ccbdd6A"
DisputeGameFactoryProxy = "0x658656A14AFdf9c507096aC406564497d13EC754"
"""
        mock_response.raise_for_status.return_value = None
        mock_session_instance.get.return_value = mock_response

        # Test fetch
        result = fetch_chain_config(mock_session_instance, "op")

        assert result is not None
        assert result["chain_name"] == "op"
        assert result["name"] == "op"
        assert result["chain_id"] == 10
        assert result["public_rpc"] == "https://mainnet.optimism.io"
        assert result["superchain_level"] == 1
        assert result["governed_by_optimism"] is True
        assert result["optimism_eip1559_elasticity"] == 3
        assert result["optimism_eip1559_denominator"] == 2
        assert result["genesis_system_config_gasLimit"] == 30000000
        assert (
            result["genesis_system_config_batcherAddress"]
            == "0x2b8733E8c60A928b19BB7db1D79b918e8E09AC8c"
        )
        assert (
            result["addresses_L1StandardBridgeProxy"]
            == "0x564Eb0CeFCcA86160649a8986C419693c82F3678"
        )

    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.new_session")
    def test_fetch_chain_config_http_error(self, mock_session):
        """Test chain config fetch with HTTP error."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance

        # Mock HTTP error
        mock_session_instance.get.side_effect = Exception("HTTP Error")

        result = fetch_chain_config(mock_session_instance, "nonexistent")

        assert result is None

    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.get_available_chains")
    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.new_session")
    @patch("op_analytics.datasources.chainsmeta.superchain.superchainconfig.ChainsMeta")
    def test_pull_superchain_config(self, mock_chains_meta, mock_session, mock_get_chains):
        """Test pulling superchain config."""
        # Mock available chains
        mock_get_chains.return_value = ["test-chain"]

        # Mock session
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance

        # Mock successful config fetch
        mock_response = Mock()
        mock_response.text = """
[chain_id]
id = 10

[optimism]
eip1559_elasticity = 3
gas_limit = 150000000
"""
        mock_response.raise_for_status.return_value = None
        mock_session_instance.get.return_value = mock_response

        # Mock ChainsMeta write
        mock_chains_meta_instance = Mock()
        mock_chains_meta.SUPERCHAIN_CONFIG = mock_chains_meta_instance

        # Test pull
        result = pull_superchain_registry()

        assert result is not None
        assert hasattr(result, "registry_df")
        assert isinstance(result.registry_df, pl.DataFrame)
        assert len(result.registry_df) > 0

    def test_schema_validation(self):
        """Test schema validation."""
        # Create test data that matches schema
        test_data = {
            "chain_name": "op",
            "name": "op",
            "chain_id": 10,
            "identifier": "mainnet/op",
            "dt": "2024-01-01",
            "optimism_eip1559_elasticity": 3,
            "genesis_system_config_gasLimit": 30000000,
            "public_rpc": "https://mainnet.optimism.io",
            "superchain_level": 1,
            "governed_by_optimism": True,
        }

        df = pl.DataFrame([test_data])

        # Should not raise exception for valid schema
        try:
            df = df.with_columns(dt=pl.lit("2024-01-01"))
            # Add missing columns with correct types
            for col in SUPERCHAIN_REGISTRY_SCHEMA.keys():
                if col not in df.columns:
                    expected_dtype = SUPERCHAIN_REGISTRY_SCHEMA[col]
                    df = df.with_columns(pl.lit(None).cast(expected_dtype).alias(col))

            # Reorder columns to match schema
            df = df.select(list(SUPERCHAIN_REGISTRY_SCHEMA.keys()))

            # Validate schema
            assert df.schema == SUPERCHAIN_REGISTRY_SCHEMA
        except Exception as e:
            pytest.fail(f"Schema validation failed: {e}")

    @patch(
        "op_analytics.datasources.chainsmeta.superchain.superchainconfig.pull_superchain_registry"
    )
    def test_execute_pull(self, mock_pull):
        """Test execute_pull function."""
        # Mock successful pull
        mock_df = pl.DataFrame({"chain_name": ["op"], "chain_id": [10]})
        mock_result = Mock()
        mock_result.registry_df = mock_df
        mock_pull.return_value = mock_result

        result = execute_pull()

        assert "registry_df" in result
        assert isinstance(result["registry_df"], dict)  # dt_summary returns dict
