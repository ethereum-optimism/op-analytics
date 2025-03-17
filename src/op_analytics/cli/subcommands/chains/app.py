import json
import os
from datetime import datetime, timedelta

import typer
from rich import print
from typing_extensions import Annotated

import op_analytics.datapipeline.rpcs as rpcs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.datapipeline.chains import goldsky_chains
from op_analytics.datapipeline.etl.blockbatch.main import compute_blockbatch
from op_analytics.datapipeline.etl.ingestion import ingest
from op_analytics.datapipeline.etl.ingestion.batches import split_block_range
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider
from op_analytics.datapipeline.etl.loadbq.main import (
    load_superchain_raw_to_bq,
    load_superchain_4337_to_bq,
)
from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION
from op_analytics.datapipeline.orchestrate import normalize_chains, normalize_blockbatch_models

log = structlog.get_logger()


app = typer.Typer(
    help="Onchain data utilities.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


@app.command()
def health():
    print("HEALTH OK.")


@app.command()
def get_block(chain: str, block_number: str):
    """Get a single block."""
    blocks = rpcs.get_block(chain, block_number)
    print(json.dumps(blocks, indent=2))


@app.command()
def get_txs(chain: str, tx_hashes: list[str]):
    """Get transactions."""
    txs = rpcs.get_transactions(chain, tx_hashes)
    print(json.dumps(txs, indent=2))


@app.command()
def get_receipts(chain: str, tx_hashes: list[str]):
    """Get transaction receipts."""
    txs = rpcs.get_receipts(chain, tx_hashes)
    print(json.dumps(txs, indent=2))


@app.command()
def goldsky_sql(
    chain: Annotated[str, typer.Argument(help="L2 chain name")],
    block_spec: Annotated[str, typer.Argument(help="Range of blocks to be filtered in the query.")],
    dataset_name: str,
):
    """Helper command to show the SQL we use to query Goldsky Clickhouse for a given block range."""

    # Split the block range into batches and use the first batch for the sql query.
    block_range = BlockRange.from_spec(block_spec)
    block_batch = split_block_range(chain, block_range)[0]

    dataset = ONCHAIN_CURRENT_VERSION[dataset_name]

    sql = dataset.goldsky_sql(
        source_table=f"{chain}_{dataset.goldsky_table_suffix}",
        where=block_batch.filter(number_column=dataset.block_number_col),
    )

    print(sql)


@app.command()
def verify_goldsky_tables():
    """Ensure Goldsky pipeline tables exist for all of the chains."""

    log.info("MAINNET CHAINS")
    mainnet = goldsky_chains.goldsky_mainnet_chains()
    goldsky_chains.verify_goldsky_tables(mainnet)

    log.info("TESTNET CHAINS")
    testnet = goldsky_chains.goldsky_testnet_chains()
    goldsky_chains.verify_goldsky_tables(testnet)


CHAINS_ARG = Annotated[str, typer.Argument(help="Comma-separated list of chains to be processed.")]

DATES_ARG = Annotated[str, typer.Argument(help="Range of dates to be processed.")]

WRITE_TO_OPTION = Annotated[
    DataLocation,
    typer.Option(
        help="Where data will be written to.",
        case_sensitive=False,
    ),
]

DRYRUN_OPTION = Annotated[
    bool, typer.Option(help="Dryrun shows a summary of the data that will be processed.")
]

FORCE_COMPLETE_OPTION = Annotated[
    bool, typer.Option(help="Run even if completion markers already exist.")
]

FORCE_NOT_READY_OPTION = Annotated[bool, typer.Option(help="Run even if inputs are notready.")]

FORK_PROCESS_OPTION = Annotated[
    bool, typer.Option(help="If true, execute task in a forked subprocess.")
]

USE_POOL_OPTION = Annotated[
    bool,
    typer.Option(help="If true, uses a process pool instead of spawning a new process per task."),
]


@app.command()
def ingest_blocks(
    chains: CHAINS_ARG,
    range_spec: Annotated[str, typer.Argument(help="Range of blocks to be ingested.")],
    read_from: Annotated[
        RawOnchainDataProvider,
        typer.Option(
            help="Where data will be read from.",
            case_sensitive=False,
        ),
    ] = RawOnchainDataProvider.GOLDSKY,
    write_to: WRITE_TO_OPTION = DataLocation.DISABLED,
    dryrun: DRYRUN_OPTION = False,
    force_complete: FORCE_COMPLETE_OPTION = False,
    fork_process: FORK_PROCESS_OPTION = True,
):
    """Ingest a range of blocks.

    Run audits + ingestion to GCS on a range of blocks.
    """
    chain_list = normalize_chains(chains)

    ingest(
        chains=chain_list,
        range_spec=range_spec,
        read_from=read_from,
        write_to=write_to,
        dryrun=dryrun,
        force_complete=force_complete,
        fork_process=fork_process,
    )


@app.command()
def blockbatch_models(
    chains: CHAINS_ARG,
    models: Annotated[str, typer.Argument(help="Comma-separated list of models to be processed.")],
    range_spec: DATES_ARG,
    read_from: Annotated[
        DataLocation,
        typer.Option(
            help="Where data will be read from.",
            case_sensitive=False,
        ),
    ] = DataLocation.GCS,
    write_to: WRITE_TO_OPTION = DataLocation.DISABLED,
    dryrun: DRYRUN_OPTION = False,
    force_complete: FORCE_COMPLETE_OPTION = False,
    fork_process: FORK_PROCESS_OPTION = True,
    use_pool: USE_POOL_OPTION = False,
):
    """Compute blockbatch models for a range of dates."""
    chain_list = normalize_chains(chains)
    model_list = normalize_blockbatch_models(models)

    compute_blockbatch(
        chains=chain_list,
        models=model_list,
        range_spec=range_spec,
        read_from=read_from,
        write_to=write_to,
        dryrun=dryrun,
        force_complete=force_complete,
        fork_process=fork_process,
        use_pool=use_pool,
    )


@app.command()
def load_superchain_raw(
    range_spec: DATES_ARG,
    dryrun: DRYRUN_OPTION = False,
    force_complete: FORCE_COMPLETE_OPTION = False,
    force_not_ready: FORCE_NOT_READY_OPTION = False,
):
    """Load superchain_raw tables to BigQuery."""
    load_superchain_raw_to_bq(
        range_spec=range_spec,
        dryrun=dryrun,
        force_complete=force_complete,
        force_not_ready=force_not_ready,
    )


@app.command()
def load_superchain_4337(
    range_spec: DATES_ARG,
    dryrun: DRYRUN_OPTION = False,
    force_complete: FORCE_COMPLETE_OPTION = False,
    force_not_ready: FORCE_NOT_READY_OPTION = False,
):
    """Load superchain_raw tables to BigQuery."""
    load_superchain_4337_to_bq(
        range_spec=range_spec,
        dryrun=dryrun,
        force_complete=force_complete,
        force_not_ready=force_not_ready,
    )


# Commands without arguments for easier k8s setup


@app.command()
def noargs_ingest():
    """No-args command to run ingestion."""

    ingest(
        chains=normalize_chains("ALL"),
        range_spec="m16hours",
        read_from=RawOnchainDataProvider.GOLDSKY,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )


@app.command()
def noargs_blockbatch():
    """No-args command to run blockbatch models."""
    compute_blockbatch(
        chains=normalize_chains("ALL"),
        models=normalize_blockbatch_models("MODELS"),
        range_spec="m16hours",
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )


@app.command()
def noargs_public_bq():
    """No-args command to load public datasets to BQ."""
    load_superchain_raw_to_bq(
        range_spec="m6days",
        dryrun=False,
        force_complete=False,
        force_not_ready=False,
    )

    load_superchain_4337_to_bq(
        range_spec="m6days",
        dryrun=False,
        force_complete=False,
        force_not_ready=False,
    )


# Backfills


@app.command()
def aa_backfill_pt2():
    """Backfill account abstraction."""
    # Kubernetes job index.
    index = int(os.environ["JOB_COMPLETION_INDEX"])

    # Num job indexes (the paralellism specified on k8s)
    num_indexes = 12

    # Define start and end dates for the backfill.
    start_date = datetime.strptime("20241201", "%Y%m%d")
    end_date = datetime.strptime("20250314", "%Y%m%d")

    # Generate date ranges with N-day intervals
    date_ranges = []
    current_date = start_date
    while current_date < end_date:
        next_date = min(current_date + timedelta(days=2), end_date)
        date_ranges.append((current_date.strftime("%Y%m%d"), next_date.strftime("%Y%m%d")))
        current_date = next_date

    for ii, (d0, d1) in enumerate(reversed(date_ranges)):
        range_spec = f"@{d0}:{d1}"
        if ii % num_indexes == index:
            compute_blockbatch(
                chains=normalize_chains("ALL,-kroma,-unichain_sepolia"),
                models=["account_abstraction_prefilter", "account_abstraction"],
                range_spec=range_spec,
                read_from=DataLocation.GCS,
                write_to=DataLocation.GCS,
                dryrun=False,
                force_complete=False,
                fork_process=True,
            )


@app.command()
def fees_backfill():
    """Backfill fees."""
    # Kubernetes job index.
    index = int(os.environ["JOB_COMPLETION_INDEX"])

    # Num job indexes (the paralellism specified on k8s)
    num_indexes = 16

    # Define start and end dates for the backfill.
    start_date = datetime.strptime("20241014", "%Y%m%d")
    end_date = datetime.strptime("20250115", "%Y%m%d")

    # Generate date ranges with N-day intervals
    date_ranges = []
    current_date = start_date
    while current_date < end_date:
        next_date = min(current_date + timedelta(days=2), end_date)
        date_ranges.append((current_date.strftime("%Y%m%d"), next_date.strftime("%Y%m%d")))
        current_date = next_date

    for ii, (d0, d1) in enumerate(reversed(date_ranges)):
        range_spec = f"@{d0}:{d1}"
        if ii % num_indexes == index:
            compute_blockbatch(
                chains=normalize_chains("ALL,-kroma,-unichain_sepolia"),
                models=["refined_traces", "aggregated_traces"],
                range_spec=range_spec,
                read_from=DataLocation.GCS,
                write_to=DataLocation.GCS,
                dryrun=False,
                force_complete=False,
                fork_process=True,
            )
