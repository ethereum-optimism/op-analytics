from op_analytics.coreutils.logger import structlog

from .location import DataLocation
from .types import PartitionedMarkerPath
from .dataaccess import init_data_access

log = structlog.get_logger()


def all_outputs_complete(
    location: DataLocation,
    markers: list[PartitionedMarkerPath],
    markers_table: str,
) -> bool:
    """Check if all outputs are complete.

    This function is somewhat low-level in that it receives the explicit completion
    markers that we are looking for. It checks that those markers are present in all
    of the data sinks.
    """
    client = init_data_access()

    complete = []
    incomplete = []

    # TODO: Make a single query for all the markers.
    for marker in markers:
        if client.marker_exists(location, marker, markers_table):
            complete.append(marker)
        else:
            incomplete.append(marker)

    log.debug(f"{len(complete)} complete, {len(incomplete)} incomplete")

    if incomplete:
        return False

    return True
