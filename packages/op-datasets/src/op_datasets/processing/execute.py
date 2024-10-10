import polars as pl
from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog
from op_coreutils.clickhouse.client import append_df


from op_datasets.processing.blockrange import BlockRange
from op_datasets.coretables.read import filter_to_date, read_core_tables
from op_datasets.processing.ozone import DateTask, split_block_range
from op_datasets.schemas import ONCHAIN_CORE_DATASETS, CoreDataset
from op_datasets.processing.write import write_to_sink

log = structlog.get_logger()


def execute(chain: str, block_spec: str, source_spec: str, sinks_spec: list[str]):
    block_range = BlockRange.from_spec(block_spec)
    clear_contextvars()
    bind_contextvars(chain=chain, spec=block_spec)
    for task in split_block_range(block_range):
        execute_single_batch(task, chain, source_spec, sinks_spec)


def execute_single_batch(batch: BlockRange, chain: str, source_spec: str, sinks_spec: list[str]):
    datasets: dict[str, CoreDataset] = {
        "blocks": ONCHAIN_CORE_DATASETS["blocks_v1"],
        "transactions": ONCHAIN_CORE_DATASETS["transactions_v1"],
        # TODO: Also read in traces, logs
    }

    input_dataframes: dict[str, pl.DataFrame] = read_core_tables(
        chain, source_spec, datasets, batch
    )

    for dt in input_dataframes["blocks"]["dt"].unique().sort().to_list():
        # We filter to a single date to make sure our processing never straddles date boundaries.
        dataframes = filter_to_date(input_dataframes, dt)

        # Determine the actual BlockRange we have in hand.
        date_task = DateTask(chain, dt, batch)
        log.info(f"Processing blocks dt={dt} min={batch.min} max={batch.max}")

        # Run the audit process.
        run_audits(dataframes)

        # Store audited datasets.
        for sink_spec in sinks_spec:
            write_all(sink_spec, date_task, "ingestion", dataframes, datasets)

        # Run data transformations.
        extractions = {}
        results = {name: logic(dataframes) for name, logic in extractions}
        log.info(f"len(results) = {len(results)}")

        # Store a record of all outputs produced.
        for output in date_task.outputs:
            log.info(f"OUTPUT: {output}")
        append_df("oplabs_monitor", "core_datasets", date_task.to_polars())


def write_all(
    sink_spec: str,
    task: DateTask,
    namespace: str,
    dataframes: dict[str, pl.DataFrame],
    datasets: dict[str, CoreDataset],
):
    for key, df in dataframes.items():
        name = datasets[key].name
        write_to_sink(sink_spec, task, namespace, name, df)


def run_audits(dataframes: dict[str, pl.DataFrame]):
    from op_datasets.logic.audits import registered_audits

    # Iterate over all the registered audits.
    # Raises an exception if an audit is failing.
    for name, audit in registered_audits.items():
        # Execute the audit!
        result: pl.DataFrame = audit(dataframes)

        if not result.collect_schema().get("audit_name") == pl.String:
            raise Exception("Audit result DataFrame is missing column: audit_name[String]")

        if not result.collect_schema().get("failure_count") == pl.UInt32:
            raise Exception("Audit result DataFrame is missing column: failure_count[UInt32]")

        for audit_result in result.to_dicts():
            name = audit_result["audit_name"]
            value = audit_result["failure_count"]

            if value > 0:
                msg = f"audit failed: {name}"
                log.error(msg)
                raise Exception(f"Audit failure {msg}")
            else:
                log.info(f"PASS audit: {name}")
