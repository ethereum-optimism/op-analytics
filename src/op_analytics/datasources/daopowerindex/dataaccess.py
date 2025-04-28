from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.clickhousedata import ClickhouseDataset

log = structlog.get_logger()


class DaoPowerIndex(ClickhouseDataset):
    # CPI snapshots
    CPI_SNAPSHOTS = "cpi_snapshots_v1"

    # CPI council percentages
    CPI_COUNCIL_PERCENTAGES = "cpi_council_percentages_v1"

    # CPI historical data
    CPI_HISTORICAL = "cpi_historical_v1"
