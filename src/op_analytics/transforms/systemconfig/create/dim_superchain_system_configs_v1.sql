CREATE TABLE IF NOT EXISTS _placeholder_
(
    -- Chain metadata from superchain_chain_list
    `chain_id` UInt32,
    `name` String,
    `identifier` String,
    `rpc` String,
    `explorers` String,
    `superchain_level` UInt32,
    `governed_by_optimism` UInt8,
    `data_availability_type` String,
    `parent_type` String,
    `parent_chain` String,
    `gas_paying_token` Nullable(FixedString(42)),
    `fault_proofs_status` String,

    -- Address information from superchain_address_list
    `address_manager` FixedString(42),
    `anchor_state_registry_proxy` Nullable(FixedString(42)),
    `batch_submitter` FixedString(42),
    `challenger` Nullable(FixedString(42)),
    `delayed_weth_proxy` Nullable(FixedString(42)),
    `dispute_game_factory_proxy` Nullable(FixedString(42)),
    `fault_dispute_game` Nullable(FixedString(42)),
    `guardian` FixedString(42),
    `l1_cross_domain_messenger_proxy` FixedString(42),
    `l1_erc721_bridge_proxy` Nullable(FixedString(42)),
    `l1_standard_bridge_proxy` FixedString(42),
    `mips` Nullable(FixedString(42)),
    `optimism_mintable_erc20_factory_proxy` Nullable(FixedString(42)),
    `optimism_portal_proxy` Nullable(FixedString(42)),
    `permissioned_dispute_game` Nullable(FixedString(42)),
    `preimage_oracle` Nullable(FixedString(42)),
    `proposer` Nullable(FixedString(42)),
    `proxy_admin` FixedString(42),
    `proxy_admin_owner` FixedString(42),
    `superchain_config` Nullable(FixedString(42)),
    `system_config_owner` FixedString(42),
    `system_config_proxy` FixedString(42),
    `unsafe_block_signer` FixedString(42),
    `l2_output_oracle_proxy` Nullable(FixedString(42)),
    `eth_lockbox_proxy` Nullable(FixedString(42)),

    -- System configuration data from system_config
    `batch_inbox_slot` Nullable(FixedString(66)),
    `dispute_game_factory_slot` Nullable(FixedString(66)),
    `l1_cross_domain_messenger_slot` Nullable(FixedString(66)),
    `l1_erc721_bridge_slot` Nullable(FixedString(66)),
    `l1_standard_bridge_slot` Nullable(FixedString(66)),
    `optimism_mintable_erc20_factory_slot` Nullable(FixedString(66)),
    `optimism_portal_slot` Nullable(FixedString(66)),
    `start_block_slot` Nullable(FixedString(66)),
    `unsafe_block_signer_slot` Nullable(FixedString(66)),
    `version` String,
    `basefee_scalar` UInt64,
    `batch_inbox` Nullable(FixedString(42)),
    `batcher_hash` Nullable(FixedString(66)),
    `eip1559_denominator` Nullable(UInt64),
    `eip1559_elasticity` Nullable(UInt64),
    `gas_limit` Nullable(UInt64),
    `maximum_gas_limit` Nullable(UInt64),
    `minimum_gas_limit` Nullable(UInt64),
    `operator_fee_constant` Nullable(UInt64),
    `operator_fee_scalar` Nullable(UInt64),
    `overhead` Nullable(UInt256),
    `scalar` Nullable(UInt256),
    `start_block` Nullable(FixedString(42)),
    `version_hex` String,

    -- Latest block information
    `latest_block_timestamp` Nullable(DateTime),
    `latest_block_number` Nullable(UInt64),

    -- Indexes for common query patterns
    INDEX chain_id_idx chain_id TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY (chain_id)
