from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Agora(DailyDataset):
    """Read data from agora.

    NOTE: We used to do dailydata ingestion of Agora data, but this was running into
    problems due to the size of the data. For that reason we redid the implementation
    using an incremental approach directly into ClickHouse.

    The datasets above won't be found in GCS. They can be found in the "transforms_governance"
    database in the OPLabs ClickHouse instance.
    """

    # Delegation events
    DELEGATE_CHANGED_EVENTS = "delegate_changed_events_v1"

    # Delegates
    DELEGATES = "delegates_v1"

    # Proposals
    PROPOSALS = "proposals_v1"

    # Vote events
    VOTES = "votes_v1"

    # Voting power snapshots
    VOTING_POWER_SNAPS = "voting_power_snaps_v1"
