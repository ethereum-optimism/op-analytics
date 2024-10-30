import io
import sys
from datetime import date
from unittest.mock import MagicMock

import polars as pl
from google.cloud import bigquery

from op_coreutils.env.aware import OPLabsEnvironment, current_environment
from op_coreutils.gcpauth import get_credentials
from op_coreutils.logger import human_rows, human_size, structlog

log = structlog.get_logger()

_CLIENT: bigquery.Client | MagicMock | None = None


def init_client():
    """BigQuery client with environment and testing awareness."""
    global _CLIENT

    if _CLIENT is None:
        current_env = current_environment()

        # Only use MagicMock in a testing environment (i.e., when running with pytest)
        if current_env == OPLabsEnvironment.PROD:
            _CLIENT = bigquery.Client(credentials=get_credentials())
        elif "pytest" in sys.modules:
            _CLIENT = MagicMock()

    # In non-PROD environments outside of tests _CLIENT remains uninitialized
    # so it raises an exception if we try to use it.
    if _CLIENT is None:
        raise RuntimeError("BigQuery client was not properly initialized.")

    return _CLIENT


class OPLabsBigQueryError(Exception):
    """Custom exception for BigQuery operations."""

    pass


def overwrite_table(df: pl.DataFrame, dataset: str, table_name: str):
    """Overwrite a BigQuery table with the given DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.

    Raises:
        OPLabsBigQueryError: If the table name does not end with '_staging' or '_latest'.
    """
    destination = f"{dataset}.{table_name}"

    if not any([table_name.endswith("_staging"), table_name.endswith("_latest")]):
        raise OPLabsBigQueryError(f"cannot overwrite data at {destination}")

    _write_df_to_bq(
        df,
        destination,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )


def overwrite_partitions_dynamic(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    expiration_days: int = 360,
):
    """Overwrite partitions in a BigQuery table.

    In dynamic mode we only overwrite those partitions that show up in the data.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        expiration_days (int, optional): Partition expiration in days. Defaults to 360.
    """

    # Ensure "dt" is a DateTime
    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    partitions = df["dt"].unique().sort().to_list()
    log.info(f"Writing {len(partitions)} partitions to BQ [{partitions[0]} ... {partitions[-1]}]")

    _write_df_to_bq(
        df,
        destination=f"{dataset}.{table_name}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="dt",  # Name of the column to use for partitioning.
                expiration_ms=expiration_days * 24 * 3600 * 1000,
            ),
        ),
    )


def overwrite_partition_static(
    df: pl.DataFrame,
    partition_dt: date,
    dataset: str,
    table_name: str,
    expiration_days: int = 360,
):
    """Overwrite single partition in a BigQuery table.

    In static mode it is expected that the dataframe does not already include a
    "dt" column. The entirety of the dataframe will be overwritten into the specified
    "partition_dt" value.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        partition_dt (date): The partition date.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        expiration_days (int, optional): Partition expiration in days. Defaults to 360.
    """
    overwrite_partitions_dynamic(
        df=df.with_columns(dt=pl.lit(partition_dt).cast(pl.Datetime)),
        dataset=dataset,
        table_name=table_name,
        expiration_days=expiration_days,
    )


def _write_df_to_bq(df: pl.DataFrame, destination: str, job_config=bigquery.LoadJobConfig):
    """Helper function to write a DataFrame to BigQuery."""
    client = init_client()

    with io.BytesIO() as stream:
        df.write_parquet(stream)
        filesize = stream.tell()
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=destination,
            job_config=job_config,
        )
        job.result()
        operation_prefix = "DRYRUN " if isinstance(client, MagicMock) else ""
        log.info(
            f"{operation_prefix}{job_config.write_disposition}: Wrote {human_rows(len(df))} {human_size(filesize)} to BQ {destination}"
        )
