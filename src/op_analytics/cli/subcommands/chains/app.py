import json

import op_datasets
import op_datasets.rpcs
import typer
from op_coreutils.logger import structlog
from op_datasets.processing.execute import execute
from typing_extensions import Annotated

log = structlog.get_logger()


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
    chain: Annotated[str, typer.Argument(help="L2 chain name")],
    block_spec: Annotated[str, typer.Argument(help="Range of blocks to be ingested.")],
    source_spec: Annotated[
        str | None, typer.Argument(help="Parameters specifying the data source.")
    ] = None,
):
    """Ingest a range of blocks [WIP].

    Runs our custom data processing functions on a range of blocks.
    """
    source_spec = source_spec or "goldsky"
    execute(chain, source_spec, block_spec)
