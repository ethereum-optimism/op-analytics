import polars as pl

from op_coreutils.gsheets import read_gsheet
from op_coreutils.clickhouse import insert_arrow


def upload_across_bridge_addresses(chains_df: pl.DataFrame):
    """Upload across bridge metadata to ClickHouse.

    - Load the data from the gsheet source of truth.
    - Verify it is consitent with Chain Metadata.
    - Upload to ClickHouse.
    """
    # Load and verify that the data is consistent with our Chain Metadata source of truth.
    df = load_across_bridge_addresses(chains_df)

    insert_arrow(
        instance="GOLDSKY",
        database="default",
        table="across_bridge_metadata_v2",
        df_arrow=df.to_arrow(),
    )


def load_across_bridge_addresses(chains_df: pl.DataFrame) -> pl.DataFrame:
    # Read CSV from Google Sheets Input
    raw_records = read_gsheet(
        location_name="across_bridge",
        worksheet_name="[INPUT -ADMIN MANAGED]",
    )
    raw_df = pl.DataFrame(raw_records, infer_schema_length=len(raw_records))

    # Ensure the dataframe is as we expect.
    assert raw_df.schema == {
        "chain_name": pl.String,
        "display_name": pl.String,
        "mainnet_chain_id": pl.Int64,
        "spokepool_address": pl.String,
    }

    # Ensure the information matches the chain metadata for goldsky_chains.
    joined_df = raw_df.join(
        chains_df.rename(
            dict(
                display_name="chain_metadata_display_name",
                mainnet_chain_id="chain_metadata_mainnet_chain_id",
            )
        ),
        left_on="chain_name",
        right_on="chain_name",
        validate="1:1",
    )

    filtered_df = joined_df.filter(
        (pl.col("display_name") != pl.col("chain_metadata_display_name"))
        | (pl.col("mainnet_chain_id") != pl.col("chain_metadata_mainnet_chain_id"))
    )

    if len(filtered_df) > 0:
        print(filtered_df)
        raise ValueError(
            "Across Bridge Addresses gsheet is inconsistent with chain metadata source of truth."
        )

    return raw_df
