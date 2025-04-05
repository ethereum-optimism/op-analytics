from op_analytics.coreutils.logger import structlog

from .dataaccess import L2Beat
from .tvsbreakdown import L2BeatTVSBreakdown

log = structlog.get_logger()


def execute_pull():
    """Pull data from L2Beat and write to GCS."""

    l2beat_tvs_breakdown = L2BeatTVSBreakdown.fetch()
    L2Beat.TVS_BREAKDOWN.write(l2beat_tvs_breakdown.df)
