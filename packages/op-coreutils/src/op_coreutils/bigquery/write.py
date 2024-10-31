import io
from datetime import date, timedelta
from unittest.mock import MagicMock

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
        expiration_days (int, optional): Partition expiration in days
    """

    # Ensure "dt" is a DateTime
    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    partitions = df["dt"].unique().sort().to_list()
    log.info(f"Writing {len(partitions)} partitions to BQ [{partitions[0]} ... {partitions[-1]}]")

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
