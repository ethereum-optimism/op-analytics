from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.datapipeline.etl.ingestion.reader.bydate import construct_readers_bydate
from op_analytics.datapipeline.etl.ingestion.reader.request import BlockBatchRequest
from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath
from op_analytics.coreutils.partitioned.location import DataLocation


log = structlog.get_logger()


def construct_readers(
    range_spec: str,
    chains: list[str],
    input_root_paths: list[str],
):
    """Insert blockbatch data into Clickhouse at a dt,chain granularity."""

    # Prepare the request for input data.
    blockbatch_request = BlockBatchRequest.build(
        chains=chains,
        range_spec=range_spec,
        root_paths_to_read=[RootPath.of(_) for _ in input_root_paths],
    )

    readers: list[DataReader] = construct_readers_bydate(
        blockbatch_request=blockbatch_request,
        read_from=DataLocation.GCS,
    )

    all_readers = {}
    date_range = DateRange.from_spec(range_spec)
    for dateval in date_range.dates():
        datestr = date_tostr(dateval)
        date_readers: list[DataReader] = [r for r in readers if r.partition_value("dt") == datestr]

        is_ready = True
        for r in date_readers:
            if not r.inputs_ready:
                log.warning(f"input data not ready for {r.partitions_dict()}")
                is_ready = False
        if not is_ready:
            # Don't do anything until the input data is completely ready for a date
            log.error(f"input data not ready for {datestr}")
            continue

        all_readers[datestr] = date_readers

    return all_readers
