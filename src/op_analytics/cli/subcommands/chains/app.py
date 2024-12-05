import json

import typer
from rich import print
from typing_extensions import Annotated

import op_analytics.datapipeline.rpcs as rpcs
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.rangeutils.blockrange import BlockRange
from op_analytics.datapipeline.chains import goldsky_chains
from op_analytics.datapipeline.chains.upload import upload_all
from op_analytics.datapipeline.etl.ingestion import ingest
from op_analytics.datapipeline.etl.ingestion.batches import split_block_range
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider
from op_analytics.datapipeline.etl.intermediate import compute_intermediate
from op_analytics.datapipeline.etl.loadbq import PipelineStage, load_to_bq
from op_analytics.datapipeline.schemas import ONCHAIN_CURRENT_VERSION

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
def chain_metadata_updates():
    """Run various chain metadata related updates.

    - Upload chain_metadata_raw.csv to Google Sheets.
    - Update the OP Analytics Chain Metadata [ADMIN MANAGED] google sheet.
    - Update the Across Superchain Bridge Addresses [ADMIN MANAGED] google sheet.

    TODO: Decide if we want to upload to Dune, Clickhouse, BigQuery. or op-analytics-static repo.
    """
    upload_all()


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


def normalize_chains(chains: str) -> list[str]:
    # If for some reason we need to force exclude a chain, add it here.
    not_included = set()

    result = set()
    for chain in chains.split(","):
        if chain == "ALL":
            result.update(goldsky_chains.goldsky_mainnet_chains())
            result.update(goldsky_chains.goldsky_testnet_chains())
        elif chain == "MAINNETS":
            result.update(goldsky_chains.goldsky_mainnet_chains())
        elif chain == "TESTNETS":
            result.update(goldsky_chains.goldsky_testnet_chains())
        elif chain.startswith("-"):
            not_included.add(chain.removeprefix("-").strip())
        else:
            result.add(chain.strip())

    excluded = result.intersection(not_included)
    for chain in excluded:
        log.warning(f"Excluding chain: {chain!r}")

    return list(result - not_included)


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
def intermediate_models(
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
):
    """Compute intermediate models for a range of dates."""
    chain_list = normalize_chains(chains)
    model_list = [_.strip() for _ in models.split(",")]

    compute_intermediate(
        chains=chain_list,
        models=model_list,
        range_spec=range_spec,
        read_from=read_from,
        write_to=write_to,
        dryrun=dryrun,
        force_complete=force_complete,
        fork_process=fork_process,
    )


@app.command()
def load_superchain_raw(
    range_spec: DATES_ARG,
    dryrun: DRYRUN_OPTION = False,
    force_complete: FORCE_COMPLETE_OPTION = False,
    force_not_ready: FORCE_NOT_READY_OPTION = False,
    write_to: Annotated[
        DataLocation,
        typer.Option(
            help="Where data will be written to.",
            case_sensitive=False,
        ),
    ] = DataLocation.BIGQUERY,
    data: Annotated[
        PipelineStage,
        typer.Option(
            help="Data that will be uploaded to BQ.",
            case_sensitive=False,
        ),
    ] = PipelineStage.RAW_ONCHAIN,
):
    """Load superchain_raw tables to BigQuery."""

    load_to_bq(
        stage=data,
        location=write_to,
        range_spec=range_spec,
        dryrun=dryrun,
        force_complete=force_complete,
        force_not_ready=force_not_ready,
    )


@app.command()
def hourly():
    """Hourly command that runs on kubernetes to keep systems current."""

    ingest(
        chains=normalize_chains("ALL"),
        range_spec="m8hours",
        read_from=RawOnchainDataProvider.GOLDSKY,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )

    for network in ["MAINNETS", "TESTNETS"]:
        compute_intermediate(
            chains=normalize_chains(network),
            models=[
                "daily_address_summary",
                "contract_creation",
            ],
            range_spec="m3days",
            read_from=DataLocation.GCS,
            write_to=DataLocation.GCS,
            dryrun=False,
            force_complete=False,
        )

    load_to_bq(
        stage=PipelineStage.RAW_ONCHAIN,
        location=DataLocation.BIGQUERY,
        range_spec="m3days",
        dryrun=False,
        force_complete=False,
        force_not_ready=False,
    )
