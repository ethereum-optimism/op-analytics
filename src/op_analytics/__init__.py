"""
op_analytics package initialization.
"""

# Import all loaders to ensure they are registered with the LoaderRegistry
from .datasources.chainsmeta import bq_chain_metadata
from .datasources.goldsky import chain_usage
from .datapipeline.chains.loaders import (
    defillama_loader,
    l2beat_loader,
    dune_loader,
    csv_loader,
    op_stack_csv_loader,
)
