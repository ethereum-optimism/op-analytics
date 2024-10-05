import polars as pl

from op_coreutils.path import repo_path
from pyiceberg.catalog.sql import SqlCatalog

from op_datasets.processing.ozone import UnitOfWork
from op_coreutils.storage.gcs import gcs_upload_parquet

WAREHOUSE_PATH = repo_path(".iceberg")
GCS_PATH = "gs://oplabs-tools-data-sink/pyiceberg"

GCS_PATH_PREFIX = "warehouse"


def write_all(task: UnitOfWork, dataframes: dict[str, pl.DataFrame]):
    for name, df in dataframes.items():
        path = f"{GCS_PATH_PREFIX}/ingestion/{task.construct_path(name)}"
        gcs_upload_parquet(path, df)


def WIP_write_all_iceberg(dataframes: dict[str, pl.DataFrame]):
    blocks = dataframes["blocks"]
    adf = blocks.to_arrow()

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
            "warehouse": GCS_PATH,
        },
    )

    catalog.create_namespace_if_not_exists("ingestion")
    table = catalog.create_table("ingestion.blocks", adf.schema)
    table.append(adf)


def WIP_iceberg_playground():
    from pyiceberg.catalog import load_catalog

    load_catalog()
    pass
