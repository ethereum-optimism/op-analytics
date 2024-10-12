import polars as pl
from op_coreutils.logger import bind_contextvars, clear_contextvars, structlog
from op_coreutils.clickhouse.client import append_df


from op_datasets.processing.blockrange import BlockRange
from op_datasets.coretables.read import read_core_datasets
from op_datasets.processing.ozone import (
    BlockBatch,
    BatchOutputs,
    split_block_range,
    split_dates,
    BatchInput,
)
from op_datasets.schemas import ONCHAIN_CORE_DATASETS, CoreDataset
from op_datasets.processing.write import write_to_sink

log = structlog.get_logger()


# The version of the core datasets we are using is configured here.
READ_CORE_DATASETS = {"blocks": "blocks_v1", "transactions": "transactions_v1"}

WRITE_CORE_DATASETS = {"blocks": "blocks_v1", "transactions": "transactions_v1"}


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
        return read_core_datasets(chain, source_spec, datasets, _block_batch)

    for block_batch in split_block_range(block_range):
        for dt, dataframes in split_dates(block_batch, data_reader):
            yield BatchInput(chain, dt, block_batch, dataframes)


def execute(chain: str, block_spec: str, source_spec: str, sinks_spec: list[str]):
    clear_contextvars()
    bind_contextvars(chain=chain, spec=block_spec)

    for microbach in reader(chain, block_spec, source_spec):
        execute_microbatch(microbach, sinks_spec)


def execute_microbatch(microbatch: BatchInput, sinks_spec: list[str]):
    log.info(f"Processing blocks dt={microbatch.dt} blocks={microbatch.min} - {microbatch.max}")

    outputs = BatchOutputs(microbatch.chain, microbatch.dt, microbatch.block_batch)

    # Run the audit process.
    run_audits(microbatch.dataframes)

    # Store audited datasets.
    for sink_spec in sinks_spec:
        write_core_datasets(sink_spec, outputs, "ingestion", microbatch.dataframes)

    # Run data transformations.
    extractions = {}
    results = {name: logic(microbatch.dataframes) for name, logic in extractions}
    log.info(f"len(results) = {len(results)}")

    # Store a record of all outputs produced.
    for output in outputs:
        log.info(f"OUTPUT: {output}")
    append_df("oplabs_monitor", "core_datasets", outputs.to_polars())


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
