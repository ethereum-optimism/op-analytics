INSERT INTO _placeholder_

WITH
superchain_address_list AS (
    SELECT * FROM dailydata_gcs.read_date(
        rootpath = 'chainsmeta/superchain_address_list_v1',
        dt = latest_dt('chainsmeta/superchain_address_list_v1')
)
)
,superchain_chain_list AS (
    SELECT * FROM dailydata_gcs.read_date(
        rootpath = 'chainsmeta/superchain_chain_list_v1',
        dt = latest_dt('chainsmeta/superchain_chain_list_v1')
    )
)
,system_config AS (
    SELECT * FROM dailydata_gcs.read_date(
        rootpath = 'chainsmeta/system_config_v1',
        dt = latest_dt('chainsmeta/system_config_v1')
    )
)
SELECT
    sl.chain_id
    ,sl.name
    ,sl.identifier
    ,sl.rpc[1] AS rpc
    ,sl.explorers[1] AS explorers
    ,sl.superchain_level
    ,sl.governed_by_optimism
    ,sl.data_availability_type
    ,sl.parent_type
    ,sl.parent_chain
    ,sl.gas_paying_token
    ,sl.fault_proofs_status
    ,al.address_manager
    ,al.anchor_state_registry_proxy
    ,al.batch_submitter
    ,al.challenger
    ,al.delayed_weth_proxy
    ,al.dispute_game_factory_proxy
    ,al.fault_dispute_game
    ,al.guardian
    ,al.l1_cross_domain_messenger_proxy
    ,al.l1_erc721_bridge_proxy
    ,al.l1_standard_bridge_proxy
    ,al.mips
    ,al.optimism_mintable_erc20_factory_proxy
    ,al.optimism_portal_proxy
    ,al.permissioned_dispute_game
    ,al.preimage_oracle
    ,al.proposer
    ,al.proxy_admin
    ,al.proxy_admin_owner
    ,al.superchain_config
    ,al.system_config_owner
    ,al.system_config_proxy
    ,al.unsafe_block_signer
    ,al.l2_output_oracle_proxy
    ,al.eth_lockbox_proxy
    ,sc.batch_inbox_slot
    ,sc.dispute_game_factory_slot
    ,sc.l1_cross_domain_messenger_slot
    ,sc.l1_erc721_bridge_slot
    ,sc.l1_standard_bridge_slot
    ,sc.optimism_mintable_erc20_factory_slot
    ,sc.optimism_portal_slot
    ,sc.start_block_slot
    ,sc.unsafe_block_signer_slot
    ,sc.version
    ,sc.basefee_scalar
    ,sc.batch_inbox
    ,sc.batcher_hash
    ,sc.eip1559_denominator
    ,sc.eip1559_elasticity
    ,sc.gas_limit
    ,sc.maximum_gas_limit
    ,sc.minimum_gas_limit
    ,sc.operator_fee_constant
    ,sc.operator_fee_scalar
    ,sc.overhead
    ,sc.scalar
    ,sc.start_block
    ,sc.version_hex
    ,sc.block_timestamp AS latest_block_timestamp
    ,sc.block_number AS latest_block_number
FROM superchain_chain_list AS sl
LEFT JOIN system_config AS sc
    ON sc.chain_id = sl.chain_id
    AND sc.identifier = sl.identifier
LEFT JOIN superchain_address_list AS al
    ON sl.chain_id = al.chain_id
SETTINGS use_hive_partitioning = 1
