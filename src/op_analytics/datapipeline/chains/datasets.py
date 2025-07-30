"""
Chain metadata datasets for partitioned storage.

Uses the existing DailyDataset pattern for consistent partitioning and marker management.
"""

from op_analytics.coreutils.partitioned.dailydata import DailyDataset


class ChainMetadata(DailyDataset):
    """Daily partitioned chain metadata datasets."""

    L2BEAT = "l2beat"
    DEFILLAMA = "defillama"
    DUNE = "dune"
    BQ_OP_STACK = "bq_op_stack"
    BQ_GOLDSKY = "bq_goldsky"
    AGGREGATED = "aggregated"
