import io
import os
import warnings

import gcsfs
import polars as pl

from op_coreutils.logger import structlog

log = structlog.get_logger()
warnings.filterwarnings("ignore", message="Polars found a filename")

PROJECT_NAME = "oplabs-tools-data"
BUCKET_NAME = "oplabs-tools-data-sink"


_CLIENT = None
_BUCKET = None
_GCSFS_CLIENT = None

_PATH_PREFIX = "op_analytics"


def init_client():
    """Idempotent client initialization.

    This function guarantess only one global instance of the storage.Client() exists.
    """
    global _CLIENT
    global _BUCKET

    if _CLIENT is None:
        from google.cloud import storage

        _CLIENT = storage.Client()
        _BUCKET = _CLIENT.bucket(BUCKET_NAME)
        log.info(f"Initialized GCS client for bucket=gs://{BUCKET_NAME}")


def human_size(size_bytes, suffix="B"):
    """Human-readable file sizes."""
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(size_bytes) < 1000.0:
            return f"{size_bytes:3.1f}{unit}{suffix}"
        size_bytes /= 1000.0
    return f"{size_bytes:.1f}Yi{suffix}"


def gcs_upload(blob_path: str, content: bytes | str, prefix=None):
    """Uploads content to GCS."""
    init_client()

    from google.cloud.storage import Blob

    key = os.path.join(prefix or _PATH_PREFIX, blob_path)
    blob: Blob = _BUCKET.blob(key)
    blob.upload_from_string(content)
    log.info(f"Wrote {human_size(len(content))} to gs://{_BUCKET.name}/{key}")


def gcs_upload_csv(blob_path: str, df: pl.DataFrame):
    buf = io.BytesIO()
    df.write_csv(buf)
    gcs_upload(blob_path, buf.getvalue())


def init_gcsfs_client():
    """Idempotent client initialization.

    This function guarantess only one global instance of the GCSFileSystem() exists.
    """
    global _GCSFS_CLIENT

    if _GCSFS_CLIENT is None:
        _GCSFS_CLIENT = gcsfs.GCSFileSystem(project=PROJECT_NAME)
        log.info(f"Initialized gcsfs client for bucket=gs://{BUCKET_NAME}")


def gcs_upload_parquet(blob_path: str, df: pl.DataFrame):
    init_gcsfs_client()

    path = f"{BUCKET_NAME}/{blob_path}"
    with _GCSFS_CLIENT.open(path, "wb") as fobj:
        df.write_parquet(fobj)
    log.info(f"Wrote parquet file at gs://{path}")
