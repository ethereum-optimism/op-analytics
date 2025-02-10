from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class GrowThePie(DailyDataset):
    """Supported growthepie datasets.

    This class includes utilities to read data from each dataset from a notebook
    for ad-hoc use cases.
    """

    # L2 Chain Fundamentals
    FUNDAMENTALS_SUMMARY = "chains_daily_fundamentals_v1"

    # Metadata for the chains
    CHAIN_METADATA = "chains_metadata_v1"

    # Contract labels
    CONTRACT_LABELS = "gtp_contract_labels_v1"
