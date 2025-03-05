import polars as pl

from op_analytics.coreutils.gsheets import read_gsheet
from op_analytics.coreutils.misc import raise_for_schema_mismatch


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
