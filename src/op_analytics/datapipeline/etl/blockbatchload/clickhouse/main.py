from datetime import date

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import date_tostr

from .insert import InsertTask, GCSData, BlockBatch
from .markers import candidate_markers, existing_markers

log = structlog.get_logger()


def load_to_clickhouse(
    datasets: list[GCSData],
    range_spec: str | None = None,
    max_tasks: int | None = None,
):
    """Insert blockbatch data into Clickhouse.

    For the given date range we find the blockbatches that have not yet been loaded to
    Clickhouse and for each of the batches we schedule an INSERT INTO task.
    """

    # Operate over recent days.
    date_range = DateRange.from_spec(range_spec or "m4days")

    # Create the output tables if they don't exist.
    for d in datasets:
        d.create_table()

    # Candidate markers are for blockbatches produced upstream.
    root_paths = set()
    for d in datasets:
        root_paths.update(d.input_root_paths)
    root_paths = list(root_paths)

    candidate_markers_df = candidate_markers(
        date_range=date_range,
        root_paths=root_paths,
    )

    # Existing markers that have already been loaded to ClickHouse.
    existing_markers_df = existing_markers(date_range)

    # Loop over datasets and find which ones are pending.
    tasks: list[InsertTask] = []
    for d in datasets:
        # Candidate markers for this dataset.
        candidates = (
            candidate_markers_df.filter(pl.col("root_path").is_in(d.input_root_paths))
            .select(
                pl.col("root_path"),
                pl.col("chain"),
                pl.col("dt"),
                pl.col("min_block"),
                # The full data path looks like: "blockbatch/contract_creation/create_traces_v1/chain=automata/dt=2025-03-21/000010664000.parquet"
                # The partitioned path is the part after the root path: "chain=automata/dt=2025-03-21/000010664000.parquet"
                pl.col("data_path")
                .str.split(pl.col("root_path") + "/")
                .list.last()
                .alias("partitioned_path"),
            )
            .unique()
            .group_by("chain", "dt", "min_block", "partitioned_path")
            .all()
        ).to_dicts()

        completed_batches = set(
            [
                (x["chain"], x["dt"], x["min_block"])
                for x in existing_markers_df.filter(pl.col("root_path") == d.output_root_path)
                .select("chain", "dt", "min_block")
                .to_dicts()
            ]
        )

        for block_batch in candidates:
            ready = block_batch["root_path"]

            # Skip batches that don't have all the required input root_paths.
            if set(d.input_root_paths) != set(ready):
                # If needed for debugging:
                # missing = sorted(set(d.input_root_paths) - set(ready))
                continue

            # Skip batches that have already been processed.
            as_tuple = (block_batch["chain"], block_batch["dt"], block_batch["min_block"])
            if as_tuple in completed_batches:
                continue

            # Collect the pending batches.

            assert isinstance(block_batch["chain"], str)
            assert isinstance(block_batch["dt"], date)
            assert isinstance(block_batch["min_block"], int)
            assert isinstance(block_batch["partitioned_path"], str)

            insert_task = InsertTask(
                dataset=d,
                blockbatch=BlockBatch(
                    chain=block_batch["chain"],
                    dt=block_batch["dt"],
                    min_block=block_batch["min_block"],
                    partitioned_path=block_batch["partitioned_path"],
                ),
                enforce_row_count=d.enforce_row_count,
            )
            tasks.append(insert_task)

    log.info(f"Prepared {len(tasks)} insert tasks.")

    summary = []
    num_tasks = len(tasks)
    for ii, task in enumerate(tasks):
        with bound_contextvars(task=f"{ii + 1}/{num_tasks}"):
            result = task.execute()

        summary.append(
            dict(
                dt=date_tostr(task.blockbatch.dt),
                chain=task.blockbatch.chain,
                table=task.dataset.output_table_name(),
                min_block=task.blockbatch.min_block,
                data_path=f"{task.dataset.output_root_path}/{task.blockbatch.partitioned_path}",
                written_rows=result.written_rows,
            )
        )

        if max_tasks is not None and ii + 1 >= max_tasks:
            break

    return summary
