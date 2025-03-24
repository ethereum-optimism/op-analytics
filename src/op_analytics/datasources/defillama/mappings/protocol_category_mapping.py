import polars as pl

from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT

from ..dataaccess import DefiLlama
from .utils import load_data_from_gsheet


DATA_MAPPINGS_GSHEET_NAME = "data_mappings"
PROTOCOL_CATEGORY_MAPPINGS_WORKSHEET_NAME = "[Category Mappings -ADMIN MANAGED]"
PROTOCOL_CATEGORY_MAPPINGS_SCHEMA = {
    "protocol_category": pl.String,
    "aggregated_category": pl.String,
}


def execute():
    df = load_data_from_gsheet(
        gsheet_name=DATA_MAPPINGS_GSHEET_NAME,
        worksheet_name=PROTOCOL_CATEGORY_MAPPINGS_WORKSHEET_NAME,
        expected_schema=PROTOCOL_CATEGORY_MAPPINGS_SCHEMA,
    )

    # Deduplicate
    dupes = df.group_by("protocol_category").len().filter(pl.col("len") > 1)
    if len(dupes) > 0:
        print(dupes)
        raise Exception("There are duplicates in the token mappings Google Sheet!")

    # Overwrite at default dt
    DefiLlama.PROTOCOL_CATEGORY_MAPPINGS.write(dataframe=df.with_columns(dt=pl.lit(DEFAULT_DT)))

    return {PROTOCOL_CATEGORY_MAPPINGS_WORKSHEET_NAME: len(df)}
