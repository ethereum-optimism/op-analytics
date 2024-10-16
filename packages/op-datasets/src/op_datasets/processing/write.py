import os

import polars as pl
from op_coreutils.logger import structlog, human_rows, human_size
from op_coreutils.storage.gcs import gcs_upload_parquet

from op_datasets.processing.ozone import BatchOutputs

log = structlog.get_logger()


def write_to_sink(sink_spec: str, task: BatchOutputs, namespace: str, name: str, df: pl.DataFrame):
    df_write = df.drop("chain", "dt")
    path = f"warehouse/{namespace}/{task.construct_path(name)}"

    if sink_spec == "gcs":
        gcs_upload_parquet(path, df_write)
        task.save_output(namespace, name, path)

    if sink_spec.startswith("file://"):
        dirpath = sink_spec.removeprefix("file://")
        filepath = os.path.join(dirpath, path)
        parent = os.path.dirname(filepath)

        if not os.path.exists(parent):
            os.makedirs(parent)

        with open(filepath, "wb") as fobj:
            df_write.write_parquet(fobj)
            size = fobj.tell()
            log.info(f"Wrote parquet [{human_rows(len(df))} {human_size(size)}] at gs://{path}")

    if sink_spec.startswith("dummy"):
        log.info(f"Dummy sink: {len(df_write)} rows to {path}")
