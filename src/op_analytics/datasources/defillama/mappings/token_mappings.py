import polars as pl

from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT

from ..dataaccess import DefiLlama
from .utils import load_data_from_gsheet


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
        gsheet_name=DATA_MAPPINGS_GSHEET_NAME,
        worksheet_name=TOKEN_MAPPINGS_WORKSHEET_NAME,
        expected_schema=TOKEN_MAPPINGS_SCHEMA,
    )

    # Deduplicate
    dupes = df.group_by("token").len().filter(pl.col("len") > 1)
    if len(dupes) > 0:
        print(dupes)
        raise Exception("There are duplicates in the token mappings Google Sheet!")

    # Overwrite at default dt
    DefiLlama.TOKEN_MAPPINGS.write(dataframe=df.with_columns(dt=pl.lit(DEFAULT_DT)))

    return {TOKEN_MAPPINGS_WORKSHEET_NAME: len(df)}
