from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session


from ..dataaccess import ChainsMeta

log = structlog.get_logger()

URL_BASE = "https://raw.githubusercontent.com/ethereum-optimism/superchain-registry/refs/heads/main/superchain/extra/addresses/"
SUPERCHAIN_ADDRESS_LIST = "addresses.json"


SUPERCHAIN_ADDRESS_LIST_SCHEMA = pl.Schema(
    {
        "chain_id": pl.Int32,
        "address_manager": pl.String,
        "anchor_state_registry_proxy": pl.String,
        "batch_submitter": pl.String,
        "challenger": pl.String,
        "delayed_weth_proxy": pl.String,
        "dispute_game_factory_proxy": pl.String,
        "fault_dispute_game": pl.String,
        "guardian": pl.String,
        "l1_cross_domain_messenger_proxy": pl.String,
        "l1_erc721_bridge_proxy": pl.String,
        "l1_standard_bridge_proxy": pl.String,
        "mips": pl.String,
        "optimism_mintable_erc20_factory_proxy": pl.String,
        "optimism_portal_proxy": pl.String,
        "permissioned_dispute_game": pl.String,
        "preimage_oracle": pl.String,
        "proposer": pl.String,
        "proxy_admin": pl.String,
        "proxy_admin_owner": pl.String,
        "superchain_config": pl.String,
        "system_config_owner": pl.String,
        "system_config_proxy": pl.String,
        "unsafe_block_signer": pl.String,
        "l2_output_oracle_proxy": pl.String,
        "eth_lockbox_proxy": pl.String,
    }
)


@dataclass
class SuperchainAddressList:
    """Superchain address list pull from ethereum-optimism github repo."""

    address_list_df: pl.DataFrame


def execute_pull():
    result = pull_superchain_address_list()
    return {
        "address_list_df": dt_summary(result.address_list_df),
    }


def pull_superchain_address_list() -> SuperchainAddressList:
    """Pull data from ethereum-optimism github repo."""
    session = new_session()

    address_list_raw_data = get_data(session, f"{URL_BASE}{SUPERCHAIN_ADDRESS_LIST}")

    # Convert the dictionary to a list of records with chain_id
    records = []
    for chain_id, addresses in address_list_raw_data.items():
        record = {"chain_id": chain_id, **addresses}
        records.append(record)

    # Create DataFrame from the records
    address_list_raw_df = pl.DataFrame(records)

    # Flatten the schema and convert to snake case.
    address_list_df = process_metadata_pull(address_list_raw_df)

    # Check the final schema is as expected. If something changes upstream the
    # exception will warn us.
    raise_for_schema_mismatch(
        actual_schema=address_list_df.schema,
        expected_schema=SUPERCHAIN_ADDRESS_LIST_SCHEMA,
    )

    # Add dt column after schema validation
    address_list_df = address_list_df.with_columns(dt=pl.lit(DEFAULT_DT))

    address_list_df = address_list_df.select(
        pl.col("chain_id"),
        *[pl.col(col).str.to_lowercase() for col in address_list_df.columns if col != "chain_id"],
    )

    ChainsMeta.SUPERCHAIN_ADDRESS_LIST.write(
        dataframe=address_list_df,
        sort_by=["chain_id"],
    )

    return SuperchainAddressList(address_list_df=address_list_df)


def process_metadata_pull(df) -> pl.DataFrame:
    """
    Cleanup metadata from Superchain token list.
    """

    # First convert PascalCase to snake_case, handling acronyms correctly
    def convert_to_snake_case(name):
        # Handle special cases for acronyms
        name = name.replace("WETH", "Weth")
        name = name.replace("ERC721", "Erc721")
        name = name.replace("ERC20", "Erc20")
        name = name.replace("MIPS", "Mips")

        # Convert to snake_case
        result = name[0].lower()
        for char in name[1:]:
            if char.isupper():
                result += "_" + char.lower()
            else:
                result += char
        return result

    df = df.rename(convert_to_snake_case)

    df = df.with_columns(pl.col("chain_id").cast(pl.Int32)).with_columns(
        pl.col("^.*address.*$").str.to_lowercase()
    )

    return df
