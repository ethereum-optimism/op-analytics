import os
import json
from datetime import datetime, timedelta
from pathlib import Path

import typer
from rich import print
from typing_extensions import Annotated

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.datapipeline.chains import goldsky_chains
from op_analytics.datapipeline.chains.aggregator import build_all_chains_metadata
from op_analytics.datapipeline.etl.blockbatch.main import compute_blockbatch
from op_analytics.datapipeline.etl.ingestion.main import ingest
from op_analytics.datapipeline.etl.ingestion.batches import split_block_range
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider
from op_analytics.datapipeline.etl.loadbq.main import (
    load_superchain_raw_to_bq,
    load_superchain_4337_to_bq,
)
from op_analytics.datapipeline.etl.blockbatchload.main import (
    load_to_clickhouse,
)
from op_analytics.datapipeline.etl.blockbatchload.yaml_loaders import (
    load_revshare_from_addresses_to_clickhouse,
    load_revshare_to_addresses_to_clickhouse,
)
from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION
from op_analytics.datapipeline.orchestrate import normalize_chains, normalize_blockbatch_models
from op_analytics.datapipeline import rpcs

log = structlog.get_logger()


app = typer.Typer(
    help="Onchain data utilities.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)

chains_app = typer.Typer(help="Chain metadata pipeline commands")


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


@chains_app.command()
def build_metadata(
    output_bq_table: str = typer.Option(
        "analytics.chain_metadata",
        help="Target BigQuery table for aggregated metadata (format: dataset.table)",
    ),
    bq_project_id: str = typer.Option(
        "oplabs-tools-data", help="BigQuery project ID for data operations"
    ),
    bq_dataset_id: str = typer.Option("raw_data", help="BigQuery dataset ID for table operations"),
):
    """
    Build aggregated chain metadata from multiple sources.

    This command orchestrates the complete chain metadata aggregation pipeline,
    including data loading, preprocessing, combination, entity resolution,
    deduplication, enrichment, validation, and output to BigQuery.
    """
    log.info("Starting chain metadata aggregation pipeline")

    manual_mappings_file = "resources/manual_chain_mappings.csv"
    log.info(f"Using manual mappings from: {manual_mappings_file}")

    result_df = build_all_chains_metadata(
        output_bq_table=output_bq_table,
        manual_mappings_filepath=manual_mappings_file,
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
    )

    log.info(f"Chain metadata aggregation completed successfully: {result_df.height} records")


def build_metadata_command(
    output_bq_table: str,
    manual_mappings_file: str,
    bq_project_id: str,
    bq_dataset_id: str,
):
    """
    Legacy function for backward compatibility.

    Builds aggregated chain metadata from multiple sources.
    """
    log.info("Starting chain metadata aggregation pipeline (legacy)")

    # Use provided manual mappings file or fallback to hardcoded path
    mappings_file = (
        manual_mappings_file
        if Path(manual_mappings_file).exists()
        else "resources/manual_chain_mappings.csv"
    )
    log.info(f"Using manual mappings from: {mappings_file}")

    build_all_chains_metadata(
        output_bq_table=output_bq_table,
        manual_mappings_filepath=mappings_file,
        bq_project_id=bq_project_id,
        bq_dataset_id=bq_dataset_id,
    )


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
    num_indexes = 24

    # Define start and end dates for the backfill.
    start_date = datetime.strptime("20250101", "%Y%m%d")
    end_date = datetime.strptime("20250225", "%Y%m%d")

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
                chains=normalize_chains("ALL"),
                models=["refined_traces"],
                range_spec=range_spec,
                read_from=DataLocation.GCS,
                write_to=DataLocation.GCS,
                dryrun=False,
                force_complete=False,
                fork_process=True,
                raise_on_failures=False,
            )


@app.command()
def newchain_backfill():
    """Backfill ingestion for a new chain."""
    new_chain = "arenaz"

    # Kubernetes job index.
    index = int(os.environ["JOB_COMPLETION_INDEX"])

    # Num job indexes (the paralellism specified on k8s)
    num_indexes = 16

    # Define start and end dates for the backfill.
    start_date = datetime.strptime("20241111", "%Y%m%d")
    end_date = datetime.strptime("20250401", "%Y%m%d")

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
            ingest(
                chains=normalize_chains(new_chain),
                range_spec=range_spec,
                read_from=RawOnchainDataProvider.GOLDSKY,
                write_to=DataLocation.GCS,
                dryrun=False,
                force_complete=False,
                fork_process=True,
            )


@app.command()
def newchain_backfill_models():
    """Backfill models for a new chain."""
    new_chain = "arenaz"

    # Kubernetes job index.
    index = int(os.environ["JOB_COMPLETION_INDEX"])

    # Num job indexes (the paralellism specified on k8s)
    num_indexes = 16

    # Define start and end dates for the backfill.
    start_date = datetime.strptime("20241111", "%Y%m%d")
    end_date = datetime.strptime("20250401", "%Y%m%d")

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
                chains=normalize_chains(new_chain),
                models=normalize_blockbatch_models("MODELS"),
                range_spec=range_spec,
                read_from=DataLocation.GCS,
                write_to=DataLocation.GCS,
                dryrun=False,
                force_complete=False,
                fork_process=True,
            )


# ClickHouse Load Commands


@app.command()
def blockbatch_loads(
    dataset_name: Annotated[
        str,
        typer.Argument(
            help="Name of the dataset to load (e.g., 'revshare_transfers', 'contract_creation')"
        ),
    ],
    range_spec: Annotated[str, typer.Argument(help="Range of dates to be processed.")],
    dryrun: DRYRUN_OPTION = False,
):
    """Load blockbatch data to ClickHouse.
    Lower case name of the dataset is the name of the dataset in the datasets module.
    """
    # Dynamically get the dataset object from the datasets module
    import op_analytics.datapipeline.etl.blockbatchload.datasets as datasets_module

    # Convert dataset_name to the constant name (e.g., "revshare_transfers" -> "REVSHARE_TRANSFERS")
    constant_name = dataset_name.upper()

    try:
        dataset = getattr(datasets_module, constant_name)
    except AttributeError:
        # Get all available datasets for better error message
        available_datasets = [
            name.lower()
            for name in dir(datasets_module)
            if name.isupper() and not name.startswith("_")
        ]
        print(f"Error: Unknown dataset '{dataset_name}'")
        print(f"Available datasets: {', '.join(available_datasets)}")
        raise typer.Exit(1)

    # Load revshare config first if needed for revshare_transfers
    if dataset_name == "revshare_transfers":
        print("Loading revshare configuration...")
        load_revshare_from_addresses_to_clickhouse()
        load_revshare_to_addresses_to_clickhouse()

    # Load the dataset
    print(f"Loading {dataset_name}...")
    load_to_clickhouse(
        dataset=dataset,
        range_spec=range_spec,
        dry_run=dryrun,
    )


@app.command()
def load_revshare_config():
    """Load revshare configuration YAML to ClickHouse."""
    print("Loading revshare_from_addresses...")
    load_revshare_from_addresses_to_clickhouse()
    print("Loading revshare_to_addresses...")
    load_revshare_to_addresses_to_clickhouse()
    print("Revshare configuration loaded successfully.")


# Backfills
