import polars as pl

from op_analytics.coreutils.gsheets import read_gsheet
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT

from .dataaccess import DefiLlama


DATA_MAPPINGS_GSHEET_NAME = "data_mappings"
TOKEN_MAPPINGS_WORKSHEET_NAME = "[Token Mappings -ADMIN MANAGED]"

TOKEN_MAPPINGS_SCHEMA = {
    "token": pl.String,
    "token_category": pl.String,
    "project": pl.String,
    "source_protocol": pl.String,
}


def execute():
    df = load_data_from_gsheet(
        DATA_MAPPINGS_GSHEET_NAME, TOKEN_MAPPINGS_WORKSHEET_NAME, TOKEN_MAPPINGS_SCHEMA
    )

    # Deduplicate
    dupes = df.group_by("token").len().filter(pl.col("len") > 1)
    if len(dupes) > 0:
        print(dupes)
        raise Exception("There are duplicates in the token mappings Google Sheet!")

    # Overwrite at default dt
    DefiLlama.TOKEN_MAPPINGS.write(dataframe=df.with_columns(dt=pl.lit(DEFAULT_DT)))

    return {TOKEN_MAPPINGS_WORKSHEET_NAME: len(df)}


def load_data_from_gsheet(
    gsheet_name: str, worksheet_name: str, expected_schema: dict
) -> pl.DataFrame:
    # Read from Google Sheets Input
    raw_records = read_gsheet(
        location_name=gsheet_name,
        worksheet_name=worksheet_name,
    )
    raw_df = pl.DataFrame(raw_records, infer_schema_length=len(raw_records))

    # Validate schema
    raise_for_schema_mismatch(
        actual_schema=raw_df.schema,
        expected_schema=pl.Schema(expected_schema),
    )

    return raw_df
