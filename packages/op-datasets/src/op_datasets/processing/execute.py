from typing import Callable

import polars as pl
from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog, human_interval
from op_coreutils.clickhouse.client import append_df


from op_datasets.processing.blockrange import BlockRange
from op_datasets.coretables.read import read_core_datasets, filter_to_date
from op_datasets.processing.ozone import (
    BlockBatch,
    BatchOutputs,
    split_block_range,
    BatchInput,
)
from op_datasets.schemas import ONCHAIN_CORE_DATASETS, CoreDataset
from op_datasets.processing.write import write_to_sink

log = structlog.get_logger()


# The version of the core datasets we are using is configured here.
READ_CORE_DATASETS = {
    "blocks": "blocks_v1",
    "transactions": "transactions_v1",
    "logs": "logs_v1",
    "traces": "traces_v1",
}

WRITE_CORE_DATASETS = {
    "blocks": "blocks_v1",
    "transactions": "transactions_v1",
    "logs": "logs_v1",
    "traces": "traces_v1",
}


def execute(chain: str, block_spec: str, source_spec: str, sinks_spec: list[str]):
    clear_contextvars()
    bind_contextvars(chain=chain)

    for microbach in reader(chain, block_spec, source_spec):
        processor(microbach, sinks_spec)


def split_dates(
    block_batch: BlockBatch,
    data_reader: Callable[[BlockBatch], dict[str, pl.DataFrame]],
):
    input_dataframes: dict[str, pl.DataFrame] = data_reader(block_batch)

    for dt in input_dataframes["blocks"]["dt"].unique().sort().to_list():
        # We filter to a single date to make sure our processing never straddles date boundaries.
        dataframes = filter_to_date(input_dataframes, dt)

        yield dt, dataframes


def reader(chain: str, block_spec: str, source_spec: str):
    """Split work in microbatches and yield BatchInput data.

    To specify which data we want to process we must always provide:

    - chain       :  Which blockchain we are reading data from
    - block_spec  :  The range of blocks we want to consume.
    - source_spec :  Where we want to read the data from.

    The reader takes care of splitting work into microbatches, esuring that
    microbatches do no straddle date boundaries.

    The reader delegates to the read_core_tables function to figure out where
    the data should come from given the source_spec.

    Each microbatch is yielded from the reader.
    """
    block_range = BlockRange.from_spec(block_spec)

    datasets: dict[str, CoreDataset] = {
        k: ONCHAIN_CORE_DATASETS[v] for k, v in READ_CORE_DATASETS.items()
    }

    def data_reader(_block_batch: BlockBatch):
        return read_core_datasets(source_spec, datasets, _block_batch)

    for block_batch in split_block_range(chain, block_range):
        for dt, dataframes in split_dates(block_batch, data_reader):
            yield BatchInput(dt, block_batch, dataframes)


def processor(microbatch: BatchInput, sinks_spec: list[str]):
    num_blocks = microbatch.block_batch.max - microbatch.block_batch.min

    num_seconds = (
        microbatch.dataframes["blocks"]
        .select(pl.col("timestamp").max() - pl.col("timestamp").min())
        .item()
    )

    log.info(
        f"Processing {num_blocks} {microbatch.chain!r} blocks on dt={microbatch.dt} spanning {human_interval(num_seconds)} starting at block={microbatch.block_batch.min}"
    )

    out = BatchOutputs(microbatch.dt, microbatch.block_batch)

    # Run the audit process.
    run_audits(microbatch.dataframes)

    # Store audited datasets.
    for sink_spec in sinks_spec:
        write_core_datasets(sink_spec, out, "ingestion", microbatch.dataframes)

    # TODO: Run data transformations.
    # extractions = {}
    # results = {name: logic(microbatch.dataframes) for name, logic in extractions}
    # log.info(f"len(results) = {len(results)}")

    # Store a record of all outputs produced.
    append_df("oplabs_monitor", "core_datasets", out.to_polars())

    # Log the outputs to stdout.
    for output in out.outputs:
        log.info(f"OUTPUT: {output}")


def run_audits(dataframes: dict[str, pl.DataFrame]):
    from op_datasets.logic.audits import registered_audits

    # Iterate over all the registered audits.
    # Raises an exception if an audit is failing.
    passing_audits = 0
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
                passing_audits += 1

    log.info(f"PASS {passing_audits} audits.")


def write_core_datasets(
    sink_spec: str,
    outputs: BatchOutputs,
    namespace: str,
    dataframes: dict[str, pl.DataFrame],
):
    namespace = "ingestion"  # we use the ingestion namespace to store core datasets
    for key in WRITE_CORE_DATASETS:
        df = dataframes[key]
        write_to_sink(sink_spec, outputs, namespace, WRITE_CORE_DATASETS[key], df)
