# -*- coding: utf-8 -*-
import io
import sys
from datetime import datetime
from unittest.mock import MagicMock

import polars as pl
from google.cloud import bigquery

from op_coreutils.env.aware import OPLabsEnvironment, current_environment
from op_coreutils.gcpauth import get_credentials
from op_coreutils.logger import human_rows, human_size, structlog
from google.api_core.exceptions import NotFound

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


def ensure_valid_dt(df: pl.DataFrame, dt=None):
    """Ensure the DataFrame contains a valid 'dt' column and date.

    Args:
        df (pl.DataFrame): The DataFrame to validate.
        dt (str, optional): The date string to validate. If None, uses the first unique 'dt' value from the DataFrame.

    Raises:
        ValueError: If the DataFrame is empty or missing 'dt' column.
        OPLabsBigQueryError: If 'dt' is not a valid date in 'YYYY-MM-DD' format.
    """
    if df.is_empty() or "dt" not in df.columns:
        raise ValueError("DataFrame is empty or missing required 'dt' column.")

    if dt is None:
        dt_values = df["dt"].unique().to_list()
        if not dt_values:
            log.info("No valid 'dt' values found in DataFrame.")
            return
        dt = str(dt_values[0])

    if isinstance(dt, datetime) or hasattr(dt, "strftime"):
        dt = dt.strftime("%Y-%m-%d")
    elif isinstance(dt, str):
        dt = dt.split(" ")[0]
    try:
        datetime.strptime(dt, "%Y-%m-%d")
    except Exception as ex:
        raise OPLabsBigQueryError(f"invalid date partition dt={dt}") from ex


def write_df_to_bq(
    df: pl.DataFrame,
    destination: str,
    client: bigquery.Client,
    operation: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    time_partitioning: bigquery.TimePartitioning = None,
):
    """Helper function to write a DataFrame to BigQuery.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        destination (str): The BigQuery table destination in 'dataset.table' format.
        client (bigquery.Client): The BigQuery client.
        operation (str): Description of the operation for logging.
        write_disposition (str, optional): BigQuery write disposition. Defaults to 'WRITE_TRUNCATE'.
        time_partitioning (bigquery.TimePartitioning, optional): Time partitioning configuration.
    """
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        filesize = stream.tell()
        stream.seek(0)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            time_partitioning=time_partitioning,
        )
        job = client.load_table_from_file(
            stream,
            destination=destination,
            job_config=job_config,
        )
        job.result()
        operation_prefix = "DRYRUN " if isinstance(client, MagicMock) else ""
        log.info(
            f"{operation_prefix}{operation}: Wrote {human_rows(len(df))} {human_size(filesize)} to BQ {destination}"
        )


def overwrite_table(df: pl.DataFrame, dataset: str, table_name: str):
    """Overwrite a BigQuery table with the given DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.

    Raises:
        OPLabsBigQueryError: If the table name does not end with '_staging' or '_latest'.
    """
    client = init_client()
    destination = f"{dataset}.{table_name}"

    if not any([table_name.endswith("_staging"), table_name.endswith("_latest")]):
        raise OPLabsBigQueryError(f"cannot overwrite data at {destination}")

    write_df_to_bq(df, destination, client, operation="OVERWRITE TABLE")


def overwrite_partition(
    df: pl.DataFrame, dt: str, dataset: str, table_name: str, expiration_days: int = 360
):
    """Overwrite a specific partition in a BigQuery table.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dt (str): The partition date in 'YYYY-MM-DD' format.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        expiration_days (int, optional): Partition expiration in days. Defaults to 360.
    """
    ensure_valid_dt(df, dt)
    df = df.with_columns(dt=pl.lit(dt).str.strptime(pl.Datetime, "%Y-%m-%d"))
    overwrite_partitions(df, dataset, table_name, expiration_days)


def overwrite_partitions(
    df: pl.DataFrame, dataset: str, table_name: str, expiration_days: int = 360
):
    """Overwrite multiple partitions in a BigQuery table.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        expiration_days (int, optional): Partition expiration in days. Defaults to 360.
    """
    client = init_client()
    ensure_valid_dt(df)

    destination = f"{dataset}.{table_name}"

    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    partitions = df["dt"].unique().sort().to_list()
    log.info(
        f"Writing {len(partitions)} partitions to BQ [{partitions[0]} ... {partitions[-1]}]"
    )

    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="dt",
        expiration_ms=expiration_days * 24 * 3600 * 1000,
    )

    write_df_to_bq(
        df,
        destination,
        client,
        operation="OVERWRITE PARTITION",
        time_partitioning=time_partitioning,
    )


def upsert_partition(
    df: pl.DataFrame, dt: str, dataset: str, table_name: str, unique_keys=["dt"]
):
    """Upsert data into a specific partition in a BigQuery table.

    Args:
        df (pl.DataFrame): The DataFrame to upsert.
        dt (str): The partition date in 'YYYY-MM-DD' format.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        unique_keys (list, optional): Columns that uniquely identify rows. Defaults to ['dt'].
    """
    client = init_client()
    ensure_valid_dt(df, dt)
    df = df.with_columns(dt=pl.lit(dt).str.strptime(pl.Datetime, "%Y-%m-%d"))

    destination = f"{dataset}.{table_name}"
    staging_table_name = f"{table_name}_staging"
    staging_destination = f"{dataset}.{staging_table_name}"

    # Delete the staging table if it already exists
    try:
        client.delete_table(staging_destination)
    except NotFound:
        pass  # If the table does not exist, continue without error

    write_df_to_bq(df, staging_destination, client, operation="WRITE STAGING TABLE")

    # Build the merge condition using unique_keys
    merge_condition = " AND ".join([f"T.{key} = S.{key}" for key in unique_keys])

    # Exclude unique_keys from update columns
    update_columns = [col for col in df.columns if col not in unique_keys]

    merge_query = f"""
    MERGE `{destination}` T
    USING `{staging_destination}` S
    ON {merge_condition}
    WHEN MATCHED THEN
      UPDATE SET {", ".join([f"T.{col} = S.{col}" for col in update_columns])}
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(df.columns)}) VALUES ({", ".join([f'S.{col}' for col in df.columns])})
    """

    query_job = client.query(merge_query)
    query_job.result()
    client.delete_table(staging_destination)

    log.info(f"Upsert for partition {dt} completed successfully.")
