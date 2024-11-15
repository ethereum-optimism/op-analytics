import logging
import warnings

import gcsfs
import polars as pl
from fsspec.implementations.local import LocalFileSystem

from op_analytics.coreutils.gcpauth import get_credentials
from op_analytics.coreutils.logger import human_rows, human_size, structlog

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
        creds = get_credentials()
        scoped_creds = creds.with_scopes(["https://www.googleapis.com/auth/devstorage.read_write"])
        _GCSFS_CLIENT = gcsfs.GCSFileSystem(project=PROJECT_NAME, token=scoped_creds)
        log.info(f"Initialized gcsfs client for bucket=gs://{BUCKET_NAME}")

    if _GCSFS_CLIENT is None:
        raise RuntimeError("GCSFS was not properly initialized.")

    return _GCSFS_CLIENT


def gcs_upload_parquet(blob_path: str, df: pl.DataFrame):
    client = init_gcsfs_client()
    path = f"{BUCKET_NAME}/{blob_path}"
    fsclient_upload_parquet(client, path, df)


def local_upload_parquet(path: str, df: pl.DataFrame):
    client = LocalFileSystem(auto_mkdir=True)
    fsclient_upload_parquet(client, path, df)


def fsclient_upload_parquet(client, path: str, df: pl.DataFrame, log_level=logging.DEBUG):
    with client.open(path, "wb") as fobj:
        df.write_parquet(fobj)
        size = fobj.tell()
        log.log(log_level, f"Wrote parquet [{human_rows(len(df))} {human_size(size)}] at {path}")
