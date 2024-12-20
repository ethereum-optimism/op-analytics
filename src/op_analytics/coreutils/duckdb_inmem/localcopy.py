import glob
import os

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.time import now_friendly_timestamp

from .client import init_client

log = structlog.get_logger()


def dump_local_copy(view_name: str, parquet_fileprefix: str) -> str:
    """Dump a copy of the data to a single local parquet file."""
    ctx = init_client()
    client = ctx.client

    rel = client.view(view_name)
    parquet_path = local_parquet_path(parquet_fileprefix)

    log.info(f"begin dump copy to {parquet_path!r}")
    rel.write_parquet(parquet_path, compression="zstd")
    log.info(f"done dump copy to {parquet_path!r}")
    return register_local_copy(parquet_path)


def load_local_copy(parquet_fileprefix: str, timestamp: str | None = None) -> str:
    """Load a local parquet file.

    If timestamp is not provided the last created local copy will be loaded.
    """

    parquet_path = local_parquet_path(parquet_fileprefix)
    dirname = os.path.dirname(parquet_path)
    if timestamp is None:
        candidates = sorted(
            glob.glob(os.path.join(dirname, f"{parquet_fileprefix}__*.parquet")), reverse=True
        )
        if not candidates:
            raise Exception(f"no local copies found for {parquet_fileprefix!r}")
        path = candidates[0]
    else:
        path = os.path.join(dirname, f"{parquet_fileprefix}__{timestamp}.parquet")

    log.info(f"load copy from {path!r}")
    return register_local_copy(path)


def register_local_copy(path: str) -> str:
    ctx = init_client()
    client = ctx.client

    view_name = os.path.basename(path).removesuffix(".parquet")
    client.register(
        view_name=view_name,
        python_object=client.read_parquet(path),
    )

    print(client.sql("SHOW TABLES"))
    return view_name


def local_parquet_path(parquet_fileprefix: str):
    path = repo_path(f"parquet/{parquet_fileprefix}__{now_friendly_timestamp()}.parquet")
    assert path is not None
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path
