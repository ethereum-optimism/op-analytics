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
    "blocks": "blocks_v1",
    "transactions": "transactions_v1",
    "logs": "logs_v1",
    "traces": "traces_v1",
}


def resolve_core_dataset(name) -> CoreDataset:
    return ONCHAIN_CORE_DATASETS[ONCHAIN_CURRENT_VERSION[name]]


__all__ = ["ONCHAIN_SCHEMAS", "CoreDataset"]
