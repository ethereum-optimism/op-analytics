from .core import CoreDataset
from .blocks.v1 import BLOCKS_V1_SCHEMA
from .transactions.v1 import TRANSACTIONS_V1_SCHEMA


ONCHAIN_CORE_DATASETS = {
    "blocks_v1": BLOCKS_V1_SCHEMA,
    "transactions_v1": TRANSACTIONS_V1_SCHEMA,
}

__all__ = ["ONCHAIN_SCHEMAS", "CoreDataset"]
