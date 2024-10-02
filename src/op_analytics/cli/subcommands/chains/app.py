import json
from typing_extensions import Annotated

import op_datasets
import op_datasets.rpcs
import polars as pl
import typer
from op_coreutils.logger import LOGGER
from op_datasets.coretables.fromgoldsky import get_core_tables

log = LOGGER.get_logger()


app = typer.Typer(help="Onchain data utilities.", add_completion=False)


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
def process_blocks(
    block_range: Annotated[str, typer.Argument(help="Range of blocks to be processed.")],
):
    """Process a range of blocks.

    Runs our custom data processing functions on a range of blocks.
    """
    dataframes = get_core_tables(block_range)

    ctx = pl.SQLContext()
    for name, df in dataframes.items():
        ctx.register(name=name, frame=df)

    extractions = []

    results = []
    for extraction in extractions:
        results.append(extraction(ctx, dataframes))
