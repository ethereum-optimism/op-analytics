import typer
from op_coreutils.logger import structlog

log = structlog.get_logger()


app = typer.Typer(help="Onchain data utilities.", add_completion=False)


@app.command()
def l2beat_tvl():
    """Pull TVL data from L2 beat."""
