import polars as pl

from op_analytics.coreutils.gsheets import read_gsheet
from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table

from .dataaccess import DefiLlama

BQ_DATASET = "dailydata_defillama"

TOKEN_MAPPINGS_GSHEET_NAME = "token_mappings"
TOKEN_MAPPINGS_TABLE = "token_mappings"
TOKEN_MAPPINGS_SCHEMA = {
    "token": pl.String,
    "token_category": pl.String,
    "project": pl.String,
    "source_protocol": pl.String,
}


def execute():
    df = load_data_from_gsheet(TOKEN_MAPPINGS_GSHEET_NAME, TOKEN_MAPPINGS_SCHEMA)

    DefiLlama.TOKEN_MAPPINGS.write_bq_direct(df)
    # upload_dataframe(df, TOKEN_MAPPINGS_TABLE)

    return {TOKEN_MAPPINGS_GSHEET_NAME: len(df)}


def load_data_from_gsheet(gsheet_name: str, expected_schema: dict) -> pl.DataFrame:
    # Read from Google Sheets Input
    raw_records = read_gsheet(
        location_name=gsheet_name,
        worksheet_name="[INPUT -ADMIN MANAGED]",
    )
    raw_df = pl.DataFrame(raw_records, infer_schema_length=len(raw_records))

    # Validate schema
    assert (
        raw_df.schema == expected_schema
    ), f"Schema mismatch. Expected {expected_schema}, got {raw_df.schema}"

    return raw_df


def upload_dataframe(df: pl.DataFrame, table_name: str):
    """Upload overwite a dataframe to BigQuery as an unpartitioned table."""
    overwrite_unpartitioned_table(
        df=df,
        dataset=BQ_DATASET,
        table_name=table_name,
    )
