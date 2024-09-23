import json

import op_indexer
import op_indexer.rpcs
import typer

app = typer.Typer()


@app.command()
def get_block(chain: str, block_number: str):
    blocks = op_indexer.rpcs.get_block(chain, block_number)
    print(json.dumps(blocks, indent=2))
