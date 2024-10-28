from op_coreutils.logger import structlog

from .location import DataLocation
from .types import SinkMarkerPath
from .marker import marker_exists

log = structlog.get_logger()


def all_outputs_complete(sinks: list[DataLocation], markers: list[SinkMarkerPath]) -> bool:
    """Check if all outputs are complete.

    This function is somewhat low-level in that it receives the explicit completion
    markers that we are looking for. It checks that those markers are present in all
    of the data sinks.
    """
    result = True
    for location in sinks:
        complete = []
        incomplete = []
        for marker in markers:
            if marker_exists(location, marker):
                complete.append(marker)
            else:
                incomplete.append(marker)

        log.info(f"{len(complete)} complete, {len(incomplete)} incomplete on sink={location.name}")

        if incomplete:
            log.info(f"Showing the first 5 incomplete locations at {location.name}")
            for marker_location in sorted(incomplete)[:5]:
                log.info(f"{location.name} is incomplete: {marker_location!r}")
            result = False

    return result
