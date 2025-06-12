from datetime import date
from unittest.mock import patch, MagicMock
from op_analytics.datasources.chainsmeta.systemconfig.chainsystemconfig import (
    ChainSystemConfig,
    SystemConfigList,
)


def test_chain_system_config_fetch_success(monkeypatch):
    # Mock SystemConfig and its call_rpc
    dummy_metadata = MagicMock()
    dummy_metadata.to_dict.return_value = {"scalar": 123, "overhead": 456, "version": 789}
    with patch(
        "op_analytics.datasources.chainsmeta.systemconfig.chainsystemconfig.SystemConfig"
    ) as MockSC:
        instance = MockSC.return_value
        instance.call_rpc.return_value = dummy_metadata
        csc = ChainSystemConfig(
            name="test", identifier="id", chain_id=1, rpc_url="url", system_config_proxy="proxy"
        )
        row = csc.fetch(process_dt=date(2024, 1, 1))
        assert row["name"] == "test"
        assert row["scalar"] == "123"
        assert row["overhead"] == "456"
        assert row["version"] == "789"


def test_chain_system_config_fetch_error(monkeypatch):
    with patch(
        "op_analytics.datasources.chainsmeta.systemconfig.chainsystemconfig.SystemConfig"
    ) as MockSC:
        instance = MockSC.return_value
        instance.call_rpc.return_value = None
        csc = ChainSystemConfig(
            name="test", identifier="id", chain_id=1, rpc_url="url", system_config_proxy="proxy"
        )
        row = csc.fetch(process_dt=date(2024, 1, 1))
        assert row is None


def test_system_config_list_store(monkeypatch):
    # Create dummy data
    dummy_data = [
        {
            "name": "a",
            "identifier": "b",
            "chain_id": 1,
            "rpc_url": "url",
            "system_config_proxy": "proxy",
            "block_number": 1,
            "block_timestamp": 2,
            "batch_inbox_slot": "x",
            "dispute_game_factory_slot": "y",
            "l1_cross_domain_messenger_slot": "z",
            "l1_erc721_bridge_slot": "d",
            "l1_standard_bridge_slot": "e",
            "optimism_mintable_erc20_factory_slot": "f",
            "optimism_portal_slot": "g",
            "start_block_slot": "h",
            "unsafe_block_signer_slot": "i",
            "version": "1",
            "basefee_scalar": 1,
            "batch_inbox": "a",
            "batcher_hash": "b",
            "blob_basefee_scalar": 1,
            "dispute_game_factory": "a",
            "eip1559_denominator": 1,
            "eip1559_elasticity": 1,
            "gas_limit": 1,
            "l1_cross_domain_messenger": "a",
            "l1_erc721_bridge": "a",
            "l1_standard_bridge": "a",
            "maximum_gas_limit": 1,
            "minimum_gas_limit": 1,
            "operator_fee_constant": 1,
            "operator_fee_scalar": 1,
            "optimism_mintable_erc20_factory": "a",
            "optimism_portal": "a",
            "overhead": "1",
            "owner": "a",
            "scalar": "1",
            "start_block": "a",
            "unsafe_block_signer": "a",
            "version_hex": "a",
            "dt": date(2024, 1, 1),
        }
    ]

    # Create a mock writer
    mock_writer = MagicMock()
    mock_writer.write = MagicMock()

    # Create a mock ChainsMeta class
    mock_chains_meta = MagicMock()
    mock_chains_meta.SYSTEM_CONFIG_LIST = mock_writer

    # Patch only the necessary imports
    with (
        patch(
            "op_analytics.datasources.chainsmeta.systemconfig.chainsystemconfig.ChainsMeta",
            mock_chains_meta,
        ),
        patch(
            "op_analytics.datasources.chainsmeta.systemconfig.chainsystemconfig.raise_for_schema_mismatch",
            MagicMock(),
        ),
    ):
        scl = SystemConfigList.store_system_config_data(dummy_data)
        assert isinstance(scl, SystemConfigList)
        mock_writer.write.assert_called_once()


def test_system_config_list_store_empty():
    assert SystemConfigList.store_system_config_data([]) is None
