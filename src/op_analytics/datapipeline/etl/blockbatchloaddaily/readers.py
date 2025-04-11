from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr
from op_analytics.datapipeline.etl.ingestion.reader.bydate import construct_readers_bydate
from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath

from .markers import query_blockbatch_daily_markers

log = structlog.get_logger()


@dataclass(frozen=True, order=True)
class DateChainBatch:
    """Represent a single (dt,chain) that needs to be loaded into ClickHouse."""

    dt: str
    chain: str
    partitioned_path: str

    @classmethod
    def of(cls, chain: str, dt: str | date) -> "DateChainBatch":
        if isinstance(dt, date):
            dt = date_tostr(dt)

        return DateChainBatch(
            chain=chain,
            dt=dt,
            partitioned_path=f"chain={chain}/dt={dt}/*.parquet",
        )


def construct_batches(
    range_spec: str,
    chains: list[str],
    blockbatch_root_paths: list[str],
    clickhouse_root_paths: list[str],
):
    """Construct the dt,chain batches that need to be loaded into ClickHouse.

    This method only returns batches for which the source data is verified to be
    complete and ready to process.
    """

    blockbatch_ready: list[DateChainBatch] | None = construct_blockbatch_ready(
        range_spec=range_spec,
        chains=chains,
        blockbatch_root_paths=blockbatch_root_paths,
    )

    clickhouse_ready: list[DateChainBatch] | None = construct_clickhouse_ready(
        range_spec=range_spec,
        chains=chains,
        clickhouse_root_paths=clickhouse_root_paths,
    )

    if blockbatch_ready is None and clickhouse_ready is None:
        raise RuntimeError("No inputs were specified")

    elif blockbatch_ready is None and clickhouse_ready is not None:
        ready = sorted(clickhouse_ready)

    elif blockbatch_ready is not None and clickhouse_ready is None:
        ready = sorted(blockbatch_ready)

    elif blockbatch_ready is not None and clickhouse_ready is not None:
        set_blockbatch = set(blockbatch_ready)
        set_clickhouse = set(clickhouse_ready)
        ready = sorted(list(set_blockbatch.intersection(set_clickhouse)))

    else:
        raise NotImplementedError("This should never happen")

    date_range = DateRange.from_spec(range_spec)
    log.info(f"{len(ready)} are ready to process across {len(date_range.dates())} dates")
    return ready


def construct_blockbatch_ready(
    range_spec: str,
    chains: list[str],
    blockbatch_root_paths: list[str],
):
    if not blockbatch_root_paths:
        return None

    # Get all the readers for blockbatch inputs.
    blockbatch_request = BlockBatchRequest.build(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=[RootPath.of(_) for _ in blockbatch_root_paths],
    )
    readers: list[DataReader] = construct_readers_bydate(
        blockbatch_request=blockbatch_request,
        read_from=DataLocation.GCS,
    )

    # Find the batches for which blockbatch data is ready.
    blockbatch_ready = []
    for reader in readers:
        chain = reader.partition_value("chain")
        dt = reader.partition_value("dt")

        if not reader.inputs_ready:
            log.warning(f"input data not ready for {reader.partitions_dict()}")
            continue

        blockbatch_ready.append(DateChainBatch.of(chain=chain, dt=dt))

    return blockbatch_ready


def construct_clickhouse_ready(
    range_spec: str,
    chains: list[str],
    clickhouse_root_paths: list[str],
):
    if not clickhouse_root_paths:
        return None

    # Get markers for the clickhouse inputs and find out which batches have
    # ClickHouse inputs ready.
    date_range = DateRange.from_spec(range_spec)
    input_markers_df = query_blockbatch_daily_markers(
        date_range=date_range,
        chains=chains,
        root_paths=clickhouse_root_paths,
    )
    clickhouse_markers = defaultdict(set)
    for row in input_markers_df.to_dicts():
        chain = row["chain"]
        dt = row["dt"]
        root_path = row["root_path"]
        clickhouse_markers[DateChainBatch.of(chain=chain, dt=dt)].add(root_path)

    clickhouse_ready = []
    for batch, ready_paths in clickhouse_markers.items():
        if ready_paths == set(clickhouse_root_paths):
            clickhouse_ready.append(batch)

    return clickhouse_ready
