# from op.catalog.curated.dune.pipeline import PIPE_CURATED_TXS
from op.catalog.models.agg_duckdb.pipeline import PIPE_DUCKDB, AGG_DUCKDB
from op.catalog.models.agg_polars.pipeline import AGG_POLARS, PIPE_POLARS
from op.catalog.raw.dune.pipeline import PIPE_RAW_DUNE
from op.platform.dagsterkit.pipeline_asset import pipeline_asset_for_product
from op.platform.dagsterkit.partitions import DAILY

from op.catalog.raw.dune.products import (
    FEES_SRC_BINDING, GAS_SRC_BINDING, GAS_FEES_SRC_BINDING,
)


# --- RAW assets ---
fees_raw_asset = pipeline_asset_for_product(
    binding=PIPE_RAW_DUNE,
    target=FEES_SRC_BINDING.product,
    upstream_products=(),
    partitions_def=DAILY,
    product_io_resource_key="product_io",
    description="Dune fees (RAW) via pipeline.run_one",
)
gas_raw_asset = pipeline_asset_for_product(
    binding=PIPE_RAW_DUNE,
    target=GAS_SRC_BINDING.product,
    upstream_products=(),
    partitions_def=DAILY,
    product_io_resource_key="product_io",
)
gas_fees_raw_asset = pipeline_asset_for_product(
    binding=PIPE_RAW_DUNE,
    target=GAS_FEES_SRC_BINDING.product,
    upstream_products=(),
    partitions_def=DAILY,
    product_io_resource_key="product_io",
)

# --- Transform asset (DuckDB or Polars pipeline) ---
agg_duckdb_asset = pipeline_asset_for_product(
    binding=PIPE_DUCKDB,                 # or PIPE_POLARS
    target=AGG_DUCKDB,                   # or AGG_POLARS
    upstream_products=(                  # Dagster orchestration deps
        FEES_SRC_BINDING.product,
        GAS_SRC_BINDING.product,
        GAS_FEES_SRC_BINDING.product,
    ),
    # ephemeral_products=(PIPE_CURATED_TXS),
    partitions_def=DAILY,
    product_io_resource_key="product_io",
)

agg_polars_asset = pipeline_asset_for_product(
    binding=PIPE_POLARS,                 # or PIPE_POLARS
    target=AGG_POLARS,                   # or AGG_POLARS
    upstream_products=(                  # Dagster orchestration deps
        FEES_SRC_BINDING.product,
        GAS_SRC_BINDING.product,
        GAS_FEES_SRC_BINDING.product,
    ),
    partitions_def=DAILY,
    product_io_resource_key="product_io",
)

ASSETS = [
    fees_raw_asset,
    gas_raw_asset,
    gas_fees_raw_asset,
    agg_duckdb_asset,
    agg_polars_asset,
]
