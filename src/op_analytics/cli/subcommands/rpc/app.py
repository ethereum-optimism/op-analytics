import json

import op_datasets
import op_datasets.rpcs
import typer

app = typer.Typer(help="Get onchain data from RPC nodes.")


@app.command()
def get_block(chain: str, block_number: str):
    blocks = op_datasets.rpcs.get_block(chain, block_number)
    print(json.dumps(blocks, indent=2))


@app.command()
def get_txs(chain: str, tx_hashes: list[str]):
    txs = op_datasets.rpcs.get_transactions(chain, tx_hashes)
    print(json.dumps(txs, indent=2))


@app.command()
def get_receipts(chain: str, tx_hashes: list[str]):
    txs = op_datasets.rpcs.get_receipts(chain, tx_hashes)
    print(json.dumps(txs, indent=2))
