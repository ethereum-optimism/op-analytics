import json

import op_datasets.rpcs
import typer
from op_coreutils.clickhouse.client import run_goldsky_query
from op_coreutils.gsheets import update_gsheet
from op_coreutils.logger import structlog
from op_datasets.chains.chain_metadata import (
    filter_to_goldsky_chains,
    load_chain_metadata,
    to_pandas,
)
from op_datasets.etl.ingestion import ingest
from op_datasets.pipeline.blockrange import BlockRange
from op_datasets.pipeline.ozone import split_block_range
from op_datasets.schemas import ONCHAIN_CURRENT_VERSION
from rich import print
from typing_extensions import Annotated

log = structlog.get_logger()


app = typer.Typer(
    help="Onchain data utilities.",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


@app.command()
def get_block(chain: str, block_number: str):
    """Get a single block."""
    blocks = op_datasets.rpcs.get_block(chain, block_number)
    print(json.dumps(blocks, indent=2))


@app.command()
def get_txs(chain: str, tx_hashes: list[str]):
    """Get transactions."""
    txs = op_datasets.rpcs.get_transactions(chain, tx_hashes)
    print(json.dumps(txs, indent=2))


@app.command()
def get_receipts(chain: str, tx_hashes: list[str]):
    """Get transaction receipts."""
    txs = op_datasets.rpcs.get_receipts(chain, tx_hashes)
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
def update_chain_metadata_gsheet():
    """Upload chain_metadata_raw.csv to Google Sheets.

    The chain_metadata_raw.csv file is maintained manually by the OP Labs team. This function
    accepts a local CSV file with raw chain metadata. It loads the data, cleans it up and uploads
    it to Google Sheets.

    TODO: Decide if we want to uplaod to Dune, Clickhouse or BigQuery.
    """
    clean_df = load_chain_metadata()
    goldsky_df = filter_to_goldsky_chains(clean_df)
    update_gsheet(
        location_name="chain_metadata",
        worksheet_name="Chain Metadata",
        dataframe=to_pandas(clean_df),
    )
    update_gsheet(
        location_name="chain_metadata",
        worksheet_name="Goldsky Chains",
        dataframe=to_pandas(goldsky_df),
    )


@app.command()
def verify_goldsky_tables():
    """Ensure Goldsky pipeline tables exist for all of the chains."""
    clean_df = load_chain_metadata()
    goldsky_df = filter_to_goldsky_chains(clean_df)

    tables = []
    for chain in goldsky_df["chain_name"].to_list():
        for _, dataset in ONCHAIN_CURRENT_VERSION.items():
            tables.append(f"{chain}_{dataset.goldsky_table_suffix}")
    tables_filter = ",\n".join([f"'{t}'" for t in tables])

    query = f"""
    SELECT 
        name as table_name
    FROM system.tables
    WHERE name IN ({tables_filter})
    """
    results = run_goldsky_query(query)["table_name"].to_list()

    expected_tables = set(tables)

    missing_tables = expected_tables - set(results)

    if missing_tables:
        for name in sorted(missing_tables):
            log.error(f"ERROR: Table missing in Goldsky Clickhouse: {name!r}")
    else:
        log.info("SUCCESS: All expected tables are present in Goldsky Clickkhouse")
        for name in sorted(expected_tables):
            log.info("    " + name)

    return sorted(expected_tables)


@app.command()
def ingest_blocks(
    chains: Annotated[str, typer.Argument(help="Comma-separated list of chains to be processed.")],
    range_spec: Annotated[str, typer.Argument(help="Range of blocks to be ingested.")],
    source_from: Annotated[str | None, typer.Option(help="Data source specification.")] = None,
    sink_to: Annotated[list[str] | None, typer.Option(help="Data sink(s) specification.")] = None,
    dryrun: Annotated[
        bool, typer.Option(help="Dryrun shows a summary of the data that will be processed.")
    ] = False,
    force: Annotated[
        bool, typer.Option(help="Run the full process ignore any existing completion markers.")
    ] = False,
):
    """Ingest a range of blocks.

    Run audits + ingestion to GCS on a range of blocks.
    """
    source_spec = source_from or "goldsky"
    sinks_spec = sink_to or ["gcs"]

    if chains == "ALL":
        chain_list = verify_goldsky_tables()
    else:
        chain_list = [_.strip() for _ in chains.split(",")]

    ingest(
        chains=chain_list,
        range_spec=range_spec,
        source_spec=source_spec,
        sinks_spec=sinks_spec,
        dryrun=dryrun,
        force=force,
    )
