# -*- coding: utf-8 -*-
import io
from datetime import date, datetime, timedelta, timezone
from typing import List
from unittest.mock import MagicMock
from uuid import uuid4

import polars as pl
from google.cloud import bigquery

from op_coreutils.env.aware import OPLabsEnvironment, current_environment
from op_coreutils.gcpauth import get_credentials
from op_coreutils.logger import human_rows, human_size, structlog
from op_coreutils.time import date_fromstr

log = structlog.get_logger()

_CLIENT: bigquery.Client | MagicMock | None = None


def init_client():
    """BigQuery client with environment and testing awareness."""
    global _CLIENT

    if _CLIENT is None:
        current_env = current_environment()

        if current_env == OPLabsEnvironment.PROD:
            _CLIENT = bigquery.Client(credentials=get_credentials())
        else:
            # MagicMock is used when running tests or when the PROD env is not specified.
            # This is helpful to collect data and iterate on transformations without
            # accidentally writing data to BigQuery before the final logic is in place.
            _CLIENT = MagicMock()

    if _CLIENT is None:
        raise RuntimeError("BigQuery client was not properly initialized.")

    return _CLIENT


class OPLabsBigQueryError(Exception):
    """Custom exception for BigQuery operations."""

    pass


def overwrite_unpartitioned_table(df: pl.DataFrame, dataset: str, table_name: str):
    """Overwrite an unpartitioned BigQuery table with the given DataFrame.

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


def most_recent_dates(df: pl.DataFrame, n_dates: int) -> pl.DataFrame:
    """Limit dataframe to the most recent N dates present in the data.

    This function is helpful when doing a dynamic partition overwrite. It allows us
    to select only recent partitions to update.

    Assumes the input dataframe has a "dt" column.
    """
    delta = timedelta(days=n_dates)

    if df.schema["dt"] == pl.String():
        max_dt = date_fromstr(df.select(pl.col("dt").max()).item())
        min_dt = max_dt - delta
        return df.filter(pl.col("dt") > min_dt.strftime("%Y-%m-%d"))

    elif isinstance(df.schema["dt"], (pl.Date, pl.Datetime)):
        max_dt = df.select(pl.col("dt").max()).item()
        min_dt = max_dt - delta
        return df.filter(pl.col("dt") > min_dt)

    raise NotImplementedError()


def overwrite_partitioned_table(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    expiration_days: int | None = None,
):
    """Overwrite en entire partitioned BigQuery table.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        expiration_days (int, optional): Partition expiration in days.
    """

    # Ensure "dt" is a DateTime
    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    partitions = df["dt"].unique().sort().to_list()
    log.info(
        f"Writing {len(partitions)} partitions to BQ [{partitions[0]} ... {partitions[-1]}]"
    )

    _write_df_to_bq(
        df,
        destination=f"{dataset}.{table_name}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="dt",  # Name of the column to use for partitioning.
                expiration_ms=_days_to_ms(expiration_days),
            ),
        ),
    )


def overwrite_partitions_dynamic(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    expiration_days: int | None = None,
):
    """Overwrite partitions in a BigQuery table.

    In dynamic mode we only overwrite those partitions that show up in the data.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        expiration_days (int, optional): Partition expiration in days
    """

    # Ensure "dt" is a DateTime
    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    partitions = df["dt"].unique().sort().to_list()
    log.info(
        f"Writing {len(partitions)} partitions to BQ [{partitions[0]} ... {partitions[-1]}]"
    )

    if len(partitions) > 10:
        raise OPLabsBigQueryError(
            "Dynamic Partition Overwrite detected more than 10 partitions. Aborting."
        )

    for date_partition in partitions:
        part_df = df.filter(pl.col("dt") == date_partition)
        date_suffix = date_partition.strftime("%Y%m%d")
        _write_df_to_bq(
            part_df,
            destination=f"{dataset}.{table_name}${date_suffix}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="dt",  # Name of the column to use for partitioning.
                    expiration_ms=_days_to_ms(expiration_days),
                ),
            ),
        )


def overwrite_partition_static(
    df: pl.DataFrame,
    partition_dt: date,
    dataset: str,
    table_name: str,
    expiration_days: int | None = None,
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
        expiration_days (int, optional): Partition expiration in days
    """
    overwrite_partitions_dynamic(
        df=df.with_columns(dt=pl.lit(partition_dt).cast(pl.Datetime)),
        dataset=dataset,
        table_name=table_name,
        expiration_days=expiration_days,
    )


def _days_to_ms(days: int | None) -> int | None:
    if days is None:
        return None

    return days * 24 * 3600 * 1000


def _write_df_to_bq(
    df: pl.DataFrame, destination: str, job_config=bigquery.LoadJobConfig
):
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


def upsert_table(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    unique_keys: List[str],
    partition_dt: str = None,
    expiration_minutes: int = 30,
):
    """Upsert data into a BigQuery table.

    This function works with both partitioned and non-partitioned tables. It performs
    an upsert operation based on the provided unique keys.

    If 'partition_dt' is provided, it will set the 'dt' column in the DataFrame to this date,
    and is intended for use with partitioned tables.

    Args:
        df (pl.DataFrame): The DataFrame to upsert.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        unique_keys (list): Columns that uniquely identify rows.
        partition_dt (str, optional): The partition date in 'YYYY-MM-DD' format. Defaults to None.
        expiration_minutes (int, optional): Expiration time for the staging table in minutes.
            Defaults to 30.

    Raises:
        ValueError: If the DataFrame is empty or if unique_keys are not in the DataFrame.
    """
    if df.is_empty():
        raise ValueError("The DataFrame is empty and cannot be upserted.")

    if partition_dt is not None:
        # Ensure 'dt' column is set to partition_dt
        df = df.with_columns(
            dt=pl.lit(partition_dt).str.strptime(pl.Datetime, "%Y-%m-%d")
        )

    missing_keys = [key for key in unique_keys if key not in df.columns]
    if missing_keys:
        raise ValueError(
            f"The following unique keys are missing from the DataFrame: {missing_keys}"
        )

    client = init_client()

    # Use a dedicated staging dataset
    staging_dataset = f"{dataset}_staging"

    # Ensure staging dataset exists
    try:
        client.get_dataset(staging_dataset)
    except NotFound:
        dataset_ref = bigquery.Dataset(staging_dataset)
        client.create_dataset(dataset_ref)
        log.info(f"Created staging dataset {staging_dataset}")

    # Generate a unique staging table name
    random_suffix = uuid4().hex[:8]
    staging_table_name = f"{table_name}_staging_{random_suffix}"
    staging_destination = f"{staging_dataset}.{staging_table_name}"

    _write_df_to_bq(
        df,
        staging_destination,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
        ),
    )

    # Set expiration time on the staging table
    table = client.get_table(staging_destination)
    table.expires = datetime.now(timezone.utc) + timedelta(minutes=expiration_minutes)
    client.update_table(table, ["expires"])

    # Build the merge condition using unique_keys
    merge_condition = " AND ".join([f"T.{key} = S.{key}" for key in unique_keys])

    # Exclude unique_keys from update columns
    update_columns = [col for col in df.columns if col not in unique_keys]

    # Handle case where there are no columns to update (all columns are unique keys)
    if update_columns:
        update_statement = (
            f"UPDATE SET {', '.join([f'T.{col} = S.{col}' for col in update_columns])}"
        )
    else:
        update_statement = "NOTHING"

    merge_query = f"""
    MERGE `{dataset}.{table_name}` T
    USING `{staging_destination}` S
    ON {merge_condition}
    WHEN MATCHED THEN
      {update_statement}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join(df.columns)}) VALUES ({', '.join([f'S.{col}' for col in df.columns])})
    """

    query_job = client.query(merge_query)
    query_job.result()
    client.delete_table(staging_destination)

    log.info(f"Upsert to table {dataset}.{table_name} completed successfully.")
