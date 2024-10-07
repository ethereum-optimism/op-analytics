import io

import polars as pl

from datetime import datetime
from google.cloud import bigquery
from unittest.mock import MagicMock

from op_coreutils.logger import human_rows, human_size, structlog
from op_coreutils.env.aware import current_environment, OPLabsEnvironment

log = structlog.get_logger()


_CLIENT = None


def init_client():
    """Idempotent env-aware client initialization.

    - Guarantess only one global instance exists.
    - Uses a mock when not running in PROD.
    """
    global _CLIENT

    if _CLIENT is None:
        current_env = current_environment()

        if current_env == OPLabsEnvironment.PROD:
            from google.cloud import bigquery

            _CLIENT = bigquery.Client()

        else:
            _CLIENT = MagicMock()


class OPLabsBigQueryError(Exception):
    pass


def ensure_valid_dt(dt: str):
    try:
        datetime.strptime(dt, "%Y-%m-%d")
    except Exception as ex:
        raise OPLabsBigQueryError(f"invalid date partition dt={dt}") from ex


def overwrite_table(df: pl.DataFrame, dataset: str, table_name: str):
    init_client()

    destination = f"{dataset}.{table_name}"

    # By convention we only allow overwrites on tables that end with the _staging
    # or _latest suffixes.
    if not any([table_name.endswith("_staging"), table_name.endswith("_latest")]):
        raise OPLabsBigQueryError(f"cannot overwrite data at {destination}")

    with io.BytesIO() as stream:
        df.write_parquet(stream)
        filesize = stream.tell()
        stream.seek(0)
        job = _CLIENT.load_table_from_file(
            stream,
            destination=destination,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        job.result()
        if isinstance(_CLIENT, MagicMock):
            operation = "DRYRUN OVERWRITE TABLE"
        else:
            operation = "OVERWRITE TABLE"
        log.info(
            f"{operation}: Wrote {human_rows(len(df))} {human_size(filesize)} to BQ {destination}"
        )


def overwrite_partition(
    df: pl.DataFrame,
    dt: str,
    dataset: str,
    table_name: str,
    expiration_days: int = 360,
):
    init_client()

    ensure_valid_dt(dt)
    df = df.with_columns(dt=pl.lit(dt).str.strptime(pl.Datetime, "%Y-%m-%d"))
    overwrite_partitions(df, dataset, table_name, expiration_days)


def overwrite_partitions(
    df: pl.DataFrame,
    dataset: str,
    table_name: str,
    expiration_days: int = 360,
):
    destination = f"{dataset}.{table_name}"

    if df["dt"].dtype == pl.String:
        df = df.with_columns(dt=pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d"))

    with io.BytesIO() as stream:
        df.write_parquet(stream)
        filesize = stream.tell()
        stream.seek(0)
        job = _CLIENT.load_table_from_file(
            stream,
            destination=destination,
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
        job.result()

        if isinstance(_CLIENT, MagicMock):
            operation = "DRYRUN OVERWRITE PARTITION"
        else:
            operation = "OVERWRITE PARTITION"

        log.info(
            f"{operation}: Wrote {human_rows(len(df))} {human_size(filesize)} to BQ {destination}"
        )
