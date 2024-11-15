from op_analytics.coreutils.logger import structlog

from .location import DataLocation
from .types import SinkMarkerPath
from .dataaccess import init_data_access

log = structlog.get_logger()


def all_outputs_complete(
    sinks: list[DataLocation],
    markers: list[SinkMarkerPath],
    markers_table: str,
) -> bool:
    """Check if all outputs are complete.

    This function is somewhat low-level in that it receives the explicit completion
    markers that we are looking for. It checks that those markers are present in all
    of the data sinks.
    """
    client = init_data_access()

    result = True
    for location in sinks:
        complete = []
        incomplete = []

        # TODO: Make a single query for all the markers.
        for marker in markers:
            if client.marker_exists(location, marker, markers_table):
                complete.append(marker)
            else:
                incomplete.append(marker)

        log.info(f"{len(complete)} complete, {len(incomplete)} incomplete on sink={location.name}")

        if incomplete:
            result = False

    return result
