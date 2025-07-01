"""
Revshare configuration data source access definitions.
"""

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.clickhousedata import ClickhouseDataset
from op_analytics.coreutils.clickhouse.oplabs import run_statememt_oplabs

log = structlog.get_logger()


class RevshareConfig(ClickhouseDataset):
    """Supported revshare configuration datasets."""

    # Revshare from addresses configuration
    REVSHARE_FROM_ADDRESSES = "revshare_from_addresses"

    # Revshare to addresses configuration
    REVSHARE_TO_ADDRESSES = "revshare_to_addresses"

    def write(self, dataframe):
        """Override write to ensure database exists before table creation."""
        # Create database if it doesn't exist (following transforms/create.py pattern)
        run_statememt_oplabs(f"CREATE DATABASE IF NOT EXISTS {self.db}")

        # Use the standard ClickHouseDataset write method
        super().write(dataframe)
