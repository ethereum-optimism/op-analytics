import warnings


from op_analytics.coreutils.logger import human_size, structlog

log = structlog.get_logger()
warnings.filterwarnings("ignore", message="Polars found a filename")


_CLIENT = None


def init_client():
    """Idempotent client initialization.

    This function guarantess only one global instance of the storage.Client() exists.
    """
    global _CLIENT

    if _CLIENT is None:
        from google.cloud import storage

        _CLIENT = storage.Client()
        log.info(
            "Initialized GCS client without op_analytics credentials. Can be used to access public buckets."
        )

    return _CLIENT


def gcs_upload(bucket_name: str, blob_name: str, content: bytes | str):
    """Uploads content to GCS."""
    client = init_client()

    # Lazy import to avoid slow google cloud package load.
    from google.cloud.storage import Blob

    bucket = client.bucket(bucket_name=bucket_name)
    blob: Blob = bucket.blob(blob_name=blob_name)
    blob.upload_from_string(content)
    log.info(f"Wrote {human_size(len(content))} to gs://{bucket_name}/{blob_name}")


def gcs_download_bytes(bucket_name: str, blob_name: str) -> bytes:
    """Downloads content from GCS."""
    client = init_client()

    # Lazy import to avoid slow google cloud package load.
    from google.cloud.storage import Blob

    bucket = client.bucket(bucket_name=bucket_name)
    blob: Blob = bucket.blob(blob_name=blob_name)
    return blob.download_as_bytes()
