import polars as pl
from op_coreutils.logger import structlog, bind_contextvars, clear_contextvars

from op_datasets.blockrange import BlockRange
from op_datasets.coretables.read import filter_to_date, read_core_tables
from op_datasets.datastores import DataSource
from op_datasets.processing import ingestion
from op_datasets.processing.logic import audits
from op_datasets.processing.ozone import UnitOfWork, determine_tasks

log = structlog.get_logger()


def execute(chain: str, source_spec: str, block_spec: str):
    block_range = BlockRange.from_spec(block_spec)
    clear_contextvars()
    bind_contextvars(chain=chain, spec=block_spec)
    for task in determine_tasks(block_range):
        execute_task(task, chain, source_spec)


def execute_task(task: BlockRange, chain: str, source_spec: str):
    data_source = DataSource.from_spec(source_spec)
    dataframes = read_core_tables(chain, data_source, task)

    for dt in dataframes["blocks"]["dt"].unique().sort().to_list():
        # We filter to a single date to make sure our processing never straddles date boundaries.
        dataframes = filter_to_date(dataframes, dt)

        # Determine the actual BlockRange we have in hand.
        actual_range = BlockRange(
            min=dataframes["blocks"].select("number").min().item(),
            max=dataframes["blocks"].select("number").max().item(),
        )
        dt_task = UnitOfWork(chain, dt, actual_range)
        bind_contextvars(dt=dt, b0=actual_range.min, bf=actual_range.max)
        log.info("Processing blocks")

        # Run the audit process.
        audits.run_all(dataframes)

        # Store audited datasets.
        ingestion.write_all(dt_task, dataframes)

        # Run data transformations.
        ctx = pl.SQLContext()
        for name, df in dataframes.items():
            ctx.register(name=name, frame=df)

        extractions = []
        results = []
        for extraction in extractions:
            results.append(0)
