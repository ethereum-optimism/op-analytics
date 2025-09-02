import dagster as dg
from op.platform.io.gcs.client import GcsParquetIO

class ProductIOResource(dg.ConfigurableResource):
    root: str  # e.g. "gs://my-bucket/op-data"
    storage_options: dict | None = None

    def get(self) -> GcsParquetIO:
        return GcsParquetIO(root=self.root, storage_options=self.storage_options)

@dg.resource
def product_io(init_context) -> GcsParquetIO:
    cfg = init_context.resource_config  # if using legacy resource config
    return GcsParquetIO(root=cfg["root"], storage_options=cfg.get("storage_options"))
