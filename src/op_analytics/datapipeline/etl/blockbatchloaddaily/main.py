from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import date_tostr
from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains

from ..blockbatchloadspec.loadspec import LoadSpec
from .insert import DtChainBatch, InsertTask
from .markers import existing_markers
from .readers import construct_readers

log = structlog.get_logger()


def daily_to_clickhouse(
    dataset: LoadSpec,
    range_spec: str | None = None,
    dry_run: bool = False,
):
    """Insert blockbatch data into Clickhouse at a dt,chain granularity."""

    # Operate over recent days.
    range_spec = range_spec or "m4days"

    # Create the output tables if they don't exist.
    if not dry_run:
        dataset.create_table()

    chains = goldsky_mainnet_chains()

    readers: dict[str, list[DataReader]] = construct_readers(
        range_spec=range_spec,
        chains=chains,
        input_root_paths=dataset.input_root_paths,
    )

    candidate_readers: list[DataReader] = []
    for daily_readers in readers.values():
        for reader in daily_readers:
            candidate_readers.append(reader)

    # Existing markers that have already been loaded to ClickHouse.
    date_range = DateRange.from_spec(range_spec)
    existing_markers_df = existing_markers(
        date_range=date_range,
        chains=chains,
        root_path=dataset.output_root_path,
    )
    existing_markers_tuples = set(
        (date_tostr(x["dt"]), x["chain"]) for x in existing_markers_df.to_dicts()
    )

    # Loop over readers and find which ones are pending.
    tasks: list[InsertTask] = []
    for reader in candidate_readers:
        chain = reader.partition_value("chain")
        dt = reader.partition_value("dt")

        as_tuple = (dt, chain)
        if as_tuple in existing_markers_tuples:
            continue

        insert_task = InsertTask(
            dataset=dataset,
            batch=DtChainBatch(
                chain=chain,
                dt=dt,
                partitioned_path=f"chain={chain}/dt={dt}/*.parquet",
            ),
            enforce_row_count=dataset.enforce_row_count,
        )
        tasks.append(insert_task)

    log.info(f"{len(tasks)}/{len(candidate_readers)} pending insert tasks.")

    # Sort tasks by date (should still be somewhat random by chain)
    tasks.sort(key=lambda x: x.batch.dt)

    if dry_run and tasks:
        tasks[0].dry_run()
        log.warning("DRY RUN: Only the first task is shown.")
        return

    summary = run_concurrently(
        function=lambda x: x.execute(),
        targets={t.batch.partitioned_path: t for t in tasks},
        max_workers=2,
    )

    return summary
