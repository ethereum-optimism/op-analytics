from datetime import date
from typing import Iterable

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.partition import Partition
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.datapipeline.chains.activation import is_chain_active

from .request import BlockBatchRequest, BlockBatchRequestData

log = structlog.get_logger()


def construct_readers_byblock(
    blockbatch_request: BlockBatchRequest,
    read_from: DataLocation,
) -> list[DataReader]:
    """Construct a list of DataReader for the given parameters.

    The parameters specify a set of chains, dates, and datasets that we are
    interested in processing.

    Each DataReader will be able to access the parquet data paths for the requested
    datasets (root_paths) on a single block batch.

    Readers can be used for processing by block batch.
    """

    markers_df = blockbatch_request.query_markers(location=read_from)

    readers: list[DataReader] = []
    for (chain, dateval, min_block), group_df in markers_df.group_by("chain", "dt", "min_block"):
        assert isinstance(dateval, date)
        assert isinstance(chain, str)

        with bound_contextvars(chain=chain):
            if not is_chain_active(chain, dateval):
                log.info(f"skipping inactive chain: {str(dateval)} {chain} ")
                continue

            # Check if all markers present are ready.
            input_data = is_batch_ready(
                markers_df=group_df,
                root_paths_to_check=blockbatch_request.physical_root_paths_for_chain(chain),
                storage_location=read_from,
            )

            # Update data path mapping so keys are logical paths.
            dataset_paths: dict[str, list[str]] = (
                blockbatch_request.data_paths_keyed_by_logical_path(
                    chain,
                    input_data.data_paths,
                )
            )

            extra_columns_df = group_df.select("num_blocks", "min_block", "max_block").unique()
            assert len(extra_columns_df) == 1
            extra_columns = extra_columns_df.to_dicts()[0]

            obj = DataReader(
                partitions=Partition.from_tuples(
                    [
                        ("chain", chain),
                        ("dt", dateval.strftime("%Y-%m-%d")),
                    ]
                ),
                read_from=read_from,
                dataset_paths=dataset_paths,
                inputs_ready=input_data.is_complete,
                extra_marker_data=extra_columns,
            )

            readers.append(obj)

    log.info(f"prepared {len(readers)} input batches.")

    def _sort(x: DataReader):
        assert x.extra_marker_data is not None
        return (x.partition_value("chain"), x.extra_marker_data["min_block"])

    return sorted(readers, key=_sort)


def is_batch_ready(
    markers_df: pl.DataFrame,
    root_paths_to_check: Iterable[str],
    storage_location: DataLocation,
) -> BlockBatchRequestData:
    """Decide if the input data for a given block batch is complete.

    If the input data is complete, returns a map from root_path to list of parquet
    data paths for each root_path.

    If the input data is not complete returns None instead of the paths dict.
    """

    assert dict(markers_df.schema) == {
        "dt": pl.Date,
        "chain": pl.String,
        "marker_path": pl.String,
        "num_parts": pl.UInt32,
        "num_blocks": pl.Int32,
        "min_block": pl.Int64,
        "max_block": pl.Int64,
        "root_path": pl.String,
        "data_path": pl.String,
    }

    dataset_paths = {}
    for root_path in root_paths_to_check:
        dataset_df = markers_df.filter(pl.col("root_path") == root_path)

        if dataset_df.is_empty():
            return BlockBatchRequestData(
                is_complete=False,
                data_paths=None,
            )

        parquet_paths = []
        for row in dataset_df.to_dicts():
            parquet_paths.append(storage_location.absolute(row["data_path"]))

        dataset_paths[root_path] = sorted(set(parquet_paths))

    return BlockBatchRequestData(
        is_complete=True,
        data_paths=dataset_paths,
    )
