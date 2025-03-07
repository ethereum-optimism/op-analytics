from datetime import date, timedelta


from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatawritefromclickhouse import FromClickHouseWriter

from ..dataaccess import DefiLlama

log = structlog.get_logger()


def copy_to_gcs(process_dt: date, last_n_days: int):
    """Write data for the last N dates to GCS."""

    min_dt = process_dt - timedelta(days=last_n_days)

    results = []

    writer1 = FromClickHouseWriter(
        dailydata_table=DefiLlama.PROTOCOLS_TVL,
        process_dt=process_dt,
        min_dt=min_dt,
        max_dt=process_dt,
        order_by="protocol_slug, chain",
    )
    results.append(writer1.write().to_dict())

    writer2 = FromClickHouseWriter(
        dailydata_table=DefiLlama.PROTOCOLS_TOKEN_TVL,
        process_dt=process_dt,
        min_dt=min_dt,
        max_dt=process_dt,
        order_by="protocol_slug, chain, token",
    )
    results.append(writer2.write().to_dict())

    return results
