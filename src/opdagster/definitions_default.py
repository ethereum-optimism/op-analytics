import dagster as dg

from .definitions.pipelines import ASSETS
from .resources.product_io import ProductIOResource

# Configure ProductIO from env; change defaults as needed
PRODUCT_IO = ProductIOResource(
    root="file:///tmp/op-analytics",
    storage_options=None,  # {"token": "cloud"} if needed
)

defs = dg.Definitions(
    assets=ASSETS,
    resources={
        "product_io": PRODUCT_IO,
    },
)
