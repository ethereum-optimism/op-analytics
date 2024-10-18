from .core import CoreDataset
from .blocks.v1 import BLOCKS_V1_SCHEMA
from .transactions.v1 import TRANSACTIONS_V1_SCHEMA
from .logs.v1 import LOGS_V1_SCHEMA
from .traces.v1 import TRACES_V1_SCHEMA


ONCHAIN_CORE_DATASETS = {
    "blocks_v1": BLOCKS_V1_SCHEMA,
    "transactions_v1": TRANSACTIONS_V1_SCHEMA,
    "logs_v1": LOGS_V1_SCHEMA,
    "traces_v1": TRACES_V1_SCHEMA,
}

ONCHAIN_CURRENT_VERSION = {
    "blocks": BLOCKS_V1_SCHEMA,
    "transactions": TRANSACTIONS_V1_SCHEMA,
    "logs": LOGS_V1_SCHEMA,
    "traces": TRACES_V1_SCHEMA,
}


__all__ = ["ONCHAIN_SCHEMAS", "CoreDataset"]
