import json

import op_datasets
import op_datasets.rpcs
import typer
from op_coreutils.logger import structlog
from op_datasets.processing.blockrange import BlockRange
from op_datasets.processing.execute import execute
from op_datasets.processing.ozone import split_block_range
from op_datasets.schemas import resolve_core_dataset
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

    dataset = resolve_core_dataset(dataset_name)

    sql = dataset.goldsky_sql(
        source_table=f"{chain}_{dataset.goldsky_table}",
        where=block_batch.filter(number_column=dataset.block_number_col),
    )

    print(sql)


@app.command()
def process_blocks(
    chain: Annotated[str, typer.Argument(help="L2 chain name")],
    block_spec: Annotated[str, typer.Argument(help="Range of blocks to be ingested.")],
    source_spec: Annotated[str | None, typer.Option(help="Data source specification.")] = None,
    sinks_spec: Annotated[
        list[str] | None, typer.Option(help="Data sink(s) specification.")
    ] = None,
):
    """Ingest a range of blocks [WIP].

    Runs our custom data processing functions on a range of blocks.
    """
    source_spec = source_spec or "goldsky"
    sinks_spec = sinks_spec or ["gcs"]
    execute(
        chain,
        block_spec,
        source_spec,
        sinks_spec,
    )
