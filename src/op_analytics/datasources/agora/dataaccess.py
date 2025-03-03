from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Agora(DailyDataset):
    """Read data from agora.

    For now we are only reading from the public GCS bucket.
    Possibly more to come later on.
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
