from datetime import date

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_date

from ..dataaccess import DefiLlama
from .copy import copy_to_gcs
from .buffer import write_to_buffer
from .metadata import ProtocolMetadata

log = structlog.get_logger()


TVL_TABLE_LAST_N_DAYS = 360


def execute_pull(process_dt: date | None = None):
    """Daily pull protocol TVL data from DefiLlama."""

    process_dt = process_dt or now_date()

    # Fetch the list of protocols and their metadata
    metadata = ProtocolMetadata.fetch(process_dt)
    DefiLlama.PROTOCOLS_METADATA.write(
        dataframe=metadata.df,
        sort_by=["protocol_slug"],
    )

    # Create list of slugs to fetch protocol-specific data
    slugs = metadata.slugs()

    # Fetch from API And write to buffer.
    write_to_buffer(slugs, process_dt)

    # Evaluate the state of the buffer. Remove inconsistent data.
    evaluate_buffer(process_dt)

    # Copy data from buffer to GCS.
    return copy_to_gcs(process_dt=process_dt, last_n_days=TVL_TABLE_LAST_N_DAYS)
