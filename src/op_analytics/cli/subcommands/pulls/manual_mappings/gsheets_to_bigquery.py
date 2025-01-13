import polars as pl

from op_analytics.coreutils.gsheets import read_gsheet
from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table

BQ_DATASET = "temp"

TOKEN_CATEGORIES_GSHEET_NAME = "token_categories"
TOKEN_CATEGORIES_TABLE = "token_categories"
TOKEN_CATEGORIES_SCHEMA = {
    "token": pl.String,
    "token_category": pl.String,
}

TOKEN_SOURCE_PROTOCOLS_GSHEET_NAME = "token_source_protocols"
TOKEN_SOURCE_PROTOCOLS_TABLE = "token_source_protocols"
TOKEN_SOURCE_PROTOCOLS_SCHEMA = {
    "token": pl.String,
    "project": pl.String,
    "source_protocol": pl.String,
}


def upload_token_categories():
    df = load_data_from_gsheet(TOKEN_CATEGORIES_GSHEET_NAME, TOKEN_CATEGORIES_SCHEMA)
    upload_dataframe(df, TOKEN_CATEGORIES_TABLE)


def upload_token_source_protocols():
    df = load_data_from_gsheet(TOKEN_SOURCE_PROTOCOLS_GSHEET_NAME, TOKEN_SOURCE_PROTOCOLS_SCHEMA)
    upload_dataframe(df, TOKEN_SOURCE_PROTOCOLS_TABLE)


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
