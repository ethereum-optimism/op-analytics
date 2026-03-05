import pandas as pd
import polars as pl

from op_analytics.coreutils.gsheets import record_changes, update_gsheet
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT

from .across_bridge import load_across_bridge_addresses, upload_across_bridge_addresses
from .goldsky_chains import goldsky_mainnet_chains_df
from .load import load_chain_metadata

log = structlog.get_logger()

GSHEET_NAME = "chain_metadata"
BQ_DATASET = "api_table_uploads"
BQ_TABLE = "op_stack_chain_metadata"

WORKSHEET_METADATA = "Chain Metadata"
WORKSHEET_GOLDSKY_CHAINS = "Goldsky Chains"


def upload_all():
    work_done = []

    # Save the clean df to Google Sheets
    clean_df = load_chain_metadata()
    update_gsheet(
        location_name=GSHEET_NAME,
        worksheet_name=WORKSHEET_METADATA,
        dataframe=to_pandas(clean_df),
    )
    work_done.append("Updated worksheeet: {WORKSHEET_METADATA}")

    goldsky_df = goldsky_mainnet_chains_df()

    # Save goldsky chains.
    update_gsheet(
        location_name=GSHEET_NAME,
        worksheet_name=WORKSHEET_GOLDSKY_CHAINS,
        dataframe=to_pandas(goldsky_df),
    )
    work_done.append("Updated worksheeet: {WORKSHEET_GOLDSKY_CHAINS}")

    # Upload clean_df to GCS.
    from op_analytics.datasources.chainsmeta.dataaccess import ChainsMeta

    ChainsMeta.CHAIN_METADATA_GSHEET.write(clean_df.with_columns(dt=pl.lit(DEFAULT_DT)))
    work_done.append(f"Uploaded to GCS: {ChainsMeta.CHAIN_METADATA_GSHEET.table}")

    # Upload clean_df to BigQuery.
    # Convert to pandas for the BQ write to match the legacy schema expected by
    # downstream views (e.g. views.all_chains_metadata):
    # - mainnet_chain_id: STRING (legacy used convert_to_int_or_keep_string)
    # - date columns: DATETIME (pandas datetime64 → BQ DATETIME, not TIMESTAMP)
    # - block_time_sec: FLOAT64 (keep as-is)
    bq_df = _to_bq_pandas(clean_df)
    _upload_pandas_to_bq(bq_df, BQ_DATASET, BQ_TABLE)
    work_done.append(f"Uploaded to BQ: {BQ_DATASET}.{BQ_TABLE}")

    # Save a record of what was done.
    record_changes(GSHEET_NAME, messages=work_done)

    # Load across_bridge
    # (verifies that the chain names are consistent with the Chain Metadata source of truth).
    across_bridge_df = load_across_bridge_addresses(chains_df=goldsky_df)

    # Upload the across bridge addresses.
    # Makes sure they are consistent with Chain Metadata.
    upload_across_bridge_addresses(across_bridge_df=across_bridge_df)

    # Store across_bridge_df as DailyData on GCS.
    ChainsMeta.ACROSS_BRIDGE_GSHEET.write(across_bridge_df.with_columns(dt=pl.lit(DEFAULT_DT)))


_BQ_DATE_COLS = [
    "public_mainnet_launch_date",
    "op_chain_start",
    "op_governed_start",
    "migration_start",
]


def _to_bq_pandas(clean_df: pl.DataFrame) -> pd.DataFrame:
    """Convert clean Polars df to a pandas df matching the legacy BQ schema.

    Uses pandas so that timezone-naive datetimes map to BQ DATETIME (not TIMESTAMP).
    """
    pdf = clean_df.to_pandas()
    pdf["mainnet_chain_id"] = pdf["mainnet_chain_id"].astype("Int64").astype("string")
    for col in _BQ_DATE_COLS:
        if col in pdf.columns:
            pdf[col] = pd.to_datetime(pdf[col], errors="coerce")
    return pdf


def _upload_pandas_to_bq(pdf: pd.DataFrame, dataset: str, table_name: str):
    """Write a pandas DataFrame to BQ, using CSV format to preserve DATETIME types.

    We use CSV instead of Parquet because BQ interprets Parquet timestamps as TIMESTAMP
    (with timezone), but the legacy schema uses DATETIME (without timezone). CSV + explicit
    schema gives us full control over BQ column types.
    """
    import io

    from google.cloud import bigquery

    from op_analytics.coreutils.bigquery.client import init_client
    from op_analytics.coreutils.logger import human_rows, human_size

    client = init_client()
    destination = f"{dataset}.{table_name}"

    # Build BQ schema: use DATETIME for date columns, infer the rest.
    bq_schema = []
    for col in pdf.columns:
        if col in _BQ_DATE_COLS:
            bq_schema.append(bigquery.SchemaField(col, "DATETIME"))
        elif pdf[col].dtype == "float64":
            bq_schema.append(bigquery.SchemaField(col, "FLOAT64"))
        elif pdf[col].dtype == "bool":
            bq_schema.append(bigquery.SchemaField(col, "BOOLEAN"))
        else:
            bq_schema.append(bigquery.SchemaField(col, "STRING"))

    with io.BytesIO() as stream:
        pdf.to_csv(stream, index=False)
        filesize = stream.tell()
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=destination,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                schema=bq_schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        job.result()
        log.info(
            f"WRITE_TRUNCATE: Wrote {human_rows(len(pdf))} {human_size(filesize)} to BQ {destination}"
        )


def to_pandas(clean_df: pl.DataFrame) -> pd.DataFrame:
    """Special handling of some columns with null values."""
    new_cols = {}
    if "mainnet_chain_id" in clean_df.columns:
        new_cols["mainnet_chain_id"] = (
            pl.when(pl.col("mainnet_chain_id").is_null())
            .then(pl.lit("NA"))
            .otherwise(pl.col("mainnet_chain_id").cast(pl.String))
        )

    if "block_time_sec" in clean_df.columns:
        new_cols["block_time_sec"] = (
            pl.when(pl.col("block_time_sec").is_null())
            .then(pl.lit("NA"))
            .otherwise(pl.col("block_time_sec").cast(pl.String))
        )

    return clean_df.with_columns(**new_cols).to_pandas()
