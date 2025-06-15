from datetime import date

import polars as pl

from op_analytics.coreutils.time import date_tostr


def transform_system_config(
    system_config_fetched_df: pl.DataFrame,
    chains_df: pl.DataFrame,
    process_dt: date,
) -> pl.DataFrame:
    system_config_df = system_config_fetched_df.join(
        chains_df, on="chain_id", how="left"
    ).with_columns(dt=pl.lit(date_tostr(process_dt)))

    # Reorder columns
    col_order = [
        "name",
        "identifier",
        "chain_id",
        "rpc_url",
        "system_config_proxy",
        "block_number",
        "block_timestamp",
        "batch_inbox_slot",
        "dispute_game_factory_slot",
        "l1_cross_domain_messenger_slot",
        "l1_erc721_bridge_slot",
        "l1_standard_bridge_slot",
        "optimism_mintable_erc20_factory_slot",
        "optimism_portal_slot",
        "start_block_slot",
        "unsafe_block_signer_slot",
        "version",
        "basefee_scalar",
        "batch_inbox",
        "batcher_hash",
        "blob_basefee_scalar",
        "dispute_game_factory",
        "eip1559_denominator",
        "eip1559_elasticity",
        "gas_limit",
        "l1_cross_domain_messenger",
        "l1_erc721_bridge",
        "l1_standard_bridge",
        "maximum_gas_limit",
        "minimum_gas_limit",
        "operator_fee_constant",
        "operator_fee_scalar",
        "optimism_mintable_erc20_factory",
        "optimism_portal",
        "overhead",
        "owner",
        "scalar",
        "start_block",
        "unsafe_block_signer",
        "version_hex",
        "dt",
    ]
    system_config_df = system_config_df.select(
        [c for c in col_order if c in system_config_df.columns]
    )
    # Normalize address columns
    address_columns = [
        c
        for c in system_config_df.columns
        if (
            "address" in c.lower()
            or c.endswith("_proxy")
            or c
            in [
                "batch_inbox",
                "dispute_game_factory",
                "l1_cross_domain_messenger",
                "l1_erc721_bridge",
                "l1_standard_bridge",
                "optimism_mintable_erc20_factory",
                "optimism_portal",
                "owner",
                "start_block",
                "unsafe_block_signer",
            ]
        )
    ]
    for col in address_columns:
        system_config_df = system_config_df.with_columns(
            pl.col(col).map_elements(_normalize_address, return_dtype=pl.String)
        )

    return system_config_df


# --- Helpers ---
def _normalize_address(val: str | None) -> str | None:
    return val.lower() if isinstance(val, str) else val
