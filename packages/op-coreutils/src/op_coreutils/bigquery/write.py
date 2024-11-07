import io
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Generator
from unittest.mock import MagicMock
from uuid import uuid4

import polars as pl
from google.cloud import bigquery
from google.api_core import exceptions

from op_coreutils.env.aware import OPLabsEnvironment, current_environment
from op_coreutils.gcpauth import get_credentials
from op_coreutils.logger import human_rows, human_size, structlog
from op_coreutils.time import date_fromstr, now

log = structlog.get_logger()

_CLIENT: bigquery.Client | MagicMock | None = None


# This dataset is used to store staging tables used in upsert operations.
# It is already configured with a default table expiration time of 1 day.
UPSERTS_TEMP_DATASET = "temp_upserts"


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


class OPLabsUpsertTableNotExists(Exception):
    """Exception raised when an upserted table does not exist yet."""

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
        expiration_days (int, optional): If provided sets the expiration time of the table.
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
        expiration_days (int, optional): If provided sets the expiration time of the table.
    """

    for date_part in breakout_partitioned_df(df):
        _write_df_to_bq(
            date_part.date_df,
            destination=f"{dataset}.{table_name}${date_part.date_suffix}",
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
        expiration_days (int, optional): If provided sets the expiration time of the table.
    """
    overwrite_partitions_dynamic(
        df=df.with_columns(dt=pl.lit(partition_dt).cast(pl.Datetime)),
        dataset=dataset,
        table_name=table_name,
        expiration_days=expiration_days,
    )


@dataclass
class DatePart:
    """Part of a dataframe corresponding to a single 'dt' value."""

    date_df: pl.DataFrame
    date_suffix: str


def breakout_partitioned_df(df: pl.DataFrame) -> Generator[DatePart, None, None]:
    """Checks that the dataframe is suitable for writing to a partitioned table.

    Yields each date part along with the date part suffix for each date present in
    the data.
    """

    # Ensure "dt" is a DateTime
    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    partitions = df["dt"].unique().sort().to_list()

    log.info(
        f"Found {len(partitions)} partitions in dataframe [{partitions[0]} ... {partitions[-1]}]"
    )

    if len(partitions) > 10:
        raise OPLabsBigQueryError(
            "Dynamic Partition Overwrite detected more than 10 partitions. Aborting."
        )

    for date_partition in partitions:
        part_df = df.filter(pl.col("dt") == date_partition)
        date_suffix = date_partition.strftime("%Y%m%d")
        yield DatePart(date_df=part_df, date_suffix=date_suffix)


def _days_to_ms(days: int | None) -> int | None:
    if days is None:
        return None

    return days * 24 * 3600 * 1000


def _write_df_to_bq(
    df: pl.DataFrame,
    destination: str,
    job_config=bigquery.LoadJobConfig,
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


def upsert_unpartitioned_table(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    unique_keys: list[str],
    create_if_not_exists: bool = False,
):
    """Upsert data into an unpartitioned BigQuery table.

    Args:
        df (pl.DataFrame): The DataFrame to upsert.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        unique_keys (List[str]): Columns that uniquely identify rows.
        expiration_days (int, optional): If provided sets the expiration time of the table.

    Raises:
        ValueError: If the DataFrame is empty or if unique_keys are not in the DataFrame.
    """
    if "dt" in df.columns:
        raise ValueError("DataFrame should not contain 'dt' column for unpartitioned tables.")

    try:
        _upsert_df_to_bq(
            df=df,
            dataset=dataset,
            table_name=table_name,
            unique_keys=unique_keys,
        )
    except OPLabsUpsertTableNotExists:
        if create_if_not_exists:
            log.info(f"Creating new table: {dataset}.{table_name}")
            overwrite_unpartitioned_table(
                df=df,
                dataset=dataset,
                table_name=table_name,
            )
        else:
            raise


def upsert_partitioned_table(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    unique_keys: list[str],
    create_if_not_exists: bool = False,
):
    """Upsert data into a partitioned BigQuery table.

    This function will set the 'dt' column in the DataFrame to the provided partition_dt.

    Args:
        df (pl.DataFrame): The DataFrame to upsert.
        dataset (str): The BigQuery dataset name.
        table_name (str): The BigQuery table name.
        unique_keys (List[str]): Columns that uniquely identify rows.
        partition_dt (str): The partition date in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If the DataFrame is empty or if unique_keys are not in the DataFrame.
    """
    try:
        for date_part in breakout_partitioned_df(df):
            _upsert_df_to_bq(
                df=date_part.date_df,
                dataset=dataset,
                table_name=f"{table_name}${date_part.date_suffix}",
                unique_keys=unique_keys,
                # For a partitioned table the staging table name has to include the date suffix.
                staging_table_name=f"{table_name}_{date_part.date_suffix}",
            )

    except OPLabsUpsertTableNotExists:
        if create_if_not_exists:
            log.info(f"Creating new table: {dataset}.{table_name}")
            overwrite_partitioned_table(
                df=df,
                dataset=dataset,
                table_name=table_name,
            )
        else:
            raise


def _upsert_df_to_bq(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    unique_keys: list[str],
    staging_table_name: str | None = None,
):
    """Helper function to upsert data into a BigQuery table.

    Args:
        df: The DataFrame to upsert.
        dataset: The BigQuery dataset name.
        table_name: The BigQuery table name.
        unique_keys: Columns that uniquely identify rows.
        staging_table_name: Can be provided to use a custom staging table name.

    Raises:
        ValueError: If the DataFrame is empty or if unique_keys are not in the DataFrame.
    """
    if df.is_empty():
        raise ValueError("The DataFrame is empty and cannot be upserted.")

    missing_keys = [key for key in unique_keys if key not in df.columns]
    if missing_keys:
        raise ValueError(
            f"The following unique keys are missing from the DataFrame: {missing_keys}"
        )

    client = init_client()

    upsert_destination = f"{dataset}.{table_name}"

    # Ensure the upsert destination exists.
    try:
        client.get_table(upsert_destination)
    except exceptions.NotFound:
        raise OPLabsUpsertTableNotExists(
            f"Cannot upsert into a table that does not exist yet: {upsert_destination}"
        )

    # Generate a unique staging table name. Include the timestamp
    # in the name for debugging purposes.
    random_suffix = now().strftime("%Y%m%d%H%M-") + uuid4().hex[:8]

    if staging_table_name is not None:
        staging_table_name = f"{staging_table_name}_{random_suffix}"
    else:
        staging_table_name = f"{dataset}_{table_name}_{random_suffix}"

    staging_destination = f"{UPSERTS_TEMP_DATASET}.{staging_table_name}"

    # Write the incoming data to the staging table.
    _write_df_to_bq(
        df,
        staging_destination,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
        ),
    )

    # The staging table is not deleted after the process. We keep it around for
    # 2 hours in case it needs to be used for debugging the MERGE statement.
    table = client.get_table(staging_destination)
    table.expires = datetime.now(timezone.utc) + timedelta(hours=2)
    client.update_table(table, ["expires"])

    # Build the merge condition using unique_keys
    merge_condition = " AND ".join([f"T.{key} = S.{key}" for key in unique_keys])

    # Exclude unique_keys from update columns
    update_columns = [col for col in df.columns if col not in unique_keys]

    # Handle case where there are no columns to update (all columns are unique keys)
    if update_columns:
        update_items = ", ".join([f"T.{col} = S.{col}" for col in update_columns])
        update_statement = f"UPDATE SET {update_items}"
    else:
        update_statement = "NOTHING"

    # Build the insert clause.
    df_columns = ", ".join(df.columns)
    df_values = ", ".join([f"S.{col}" for col in df.columns])

    merge_query = f"""
    MERGE `{upsert_destination}` T
    USING `{staging_destination}` S
    ON {merge_condition}
    WHEN MATCHED THEN
      {update_statement}
    WHEN NOT MATCHED THEN
      INSERT ({df_columns}) VALUES ({df_values})
    """

    # Print the merge query for debugging purposes.
    print()
    print(merge_query)
    print()

    query_job = client.query(merge_query)
    query_job.result()

    operation_prefix = "DRYRUN " if isinstance(client, MagicMock) else ""
    log.info(f"{operation_prefix}UPSERT: {human_rows(len(df))} to BQ {upsert_destination}")
