import os
import warnings
from dataclasses import dataclass
from typing import Any

import gcsfs
import polars as pl

from op_coreutils.logger import structlog, human_size, human_rows

log = structlog.get_logger()
warnings.filterwarnings("ignore", message="Polars found a filename")

PROJECT_NAME = "oplabs-tools-data"
BUCKET_NAME = "oplabs-tools-data-sink"

_GCSFS_CLIENT = None


def init_gcsfs_client():
    """Idempotent client initialization.

    This function guarantess only one global instance of the GCSFileSystem() exists.
    """
    global _GCSFS_CLIENT

    if _GCSFS_CLIENT is None:
        _GCSFS_CLIENT = gcsfs.GCSFileSystem(project=PROJECT_NAME)
        log.info(f"Initialized gcsfs client for bucket=gs://{BUCKET_NAME}")

    if _GCSFS_CLIENT is None:
        raise RuntimeError("GCSFS was not properly initialized.")

    return _GCSFS_CLIENT


def gcs_upload_parquet(blob_path: str, df: pl.DataFrame):
    client = init_gcsfs_client()
    path = f"{BUCKET_NAME}/{blob_path}"
    fsclient_upload_parquet(client, path, df)


def fsclient_upload_parquet(client, path: str, df: pl.DataFrame):
    with client.open(path, "wb") as fobj:
        df.write_parquet(fobj)
        size = fobj.tell()
        log.info(f"Wrote parquet [{human_rows(len(df))} {human_size(size)}] at {path}")


@dataclass(order=True)
class PartitionValue:
    name: str
    value: Any


@dataclass(order=True)
class PartitionOutput:
    partition: list[PartitionValue]
    path: str
    row_count: int


def gcs_upload_partitioned_parquet(
    root_path: str, basename: str, df: pl.DataFrame, partition_cols: list[str]
) -> list[PartitionOutput]:
    client = init_gcsfs_client()

    return write_partitioned_parquet(
        filesystem=client,
        root_path=root_path,
        basename=basename,
        df=df,
        partition_cols=partition_cols,
    )


def breakout_partitions(df: pl.DataFrame, partition_cols: list[str], root_path: str, basename: str):
    parts = df.select(*partition_cols).unique().to_dicts()

    for part in parts:
        part_df = df.filter(pl.all_horizontal(pl.col(col) == val for col, val in part.items()))
        part_values = [PartitionValue(col, val) for col, val in part.items()]
        part_path = "/".join(f"{col}={val}" for col, val in part.items())

        yield (
            part_df.drop(*partition_cols),
            PartitionOutput(
                partition=part_values,
                path=os.path.join(root_path, part_path, basename),
                row_count=len(part_df),
            ),
        )


def write_partitioned_parquet(
    filesystem,
    root_path: str,
    basename: str,
    df: pl.DataFrame,
    partition_cols: list[str],
) -> list[PartitionOutput]:
    outputs = []
    for part_df, partition_output in breakout_partitions(
        df=df,
        partition_cols=partition_cols,
        root_path=root_path,
        basename=basename,
    ):
        fsclient_upload_parquet(filesystem, partition_output.path, part_df)
        outputs.append(partition_output)

    return outputs
