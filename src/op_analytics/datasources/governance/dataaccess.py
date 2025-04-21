from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.clickhousedata import ClickhouseDataset

log = structlog.get_logger()


class OverrideDB:
    """Allow overriding the database name.

    For historical reasons the database used for governance data was "transforms_governance"
    which is not the standrad location for ClickHouseDataset, which in this case would be
    "datasources_governance".

    This custom subclass overrides the db location.
    """

    db = "transforms_governance"


class Governance(OverrideDB, ClickhouseDataset):
    """Read data from agora.

    NOTE: We originally set up Agora data as a DailyDataset. This was running into problems
    due to the size of the data and the fact that it was hard to do incremental ingestion
    based on the "dt" partition (since Agora data does not have timestamps).

    We rebuilt theimplementation using ClickHouseDataset, which allows for more flexible
    partitioning. However, the data is still stored in the "transforms_governance" database.

    """

    # Delegation events
    DELEGATE_CHANGED_EVENTS = "ingest_delegate_changed_events_v1"

    # Delegates
    DELEGATES = "ingest_delegates_v1"

    # Proposals
    PROPOSALS = "ingest_proposals_v1"

    # Vote events
    VOTES = "ingest_votes_v1"

    # Voting power snapshots
    VOTING_POWER_SNAPS = "ingest_voting_power_snaps_v1"
