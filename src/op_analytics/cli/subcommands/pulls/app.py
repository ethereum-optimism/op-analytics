import typer
from op_coreutils.logger import structlog
from op_analytics.cli.subcommands.pulls.l2beat import pull as l2beat_pull
from op_analytics.cli.subcommands.pulls.defillama import pull_stables as dfl_pull_stables

log = structlog.get_logger()


app = typer.Typer(help="Pull data from 3rd party sources.", add_completion=False)


@app.command()
def l2beat():
    """Pull data from L2 beat."""
    l2beat_pull()

@app.command()
def dfl_stables():
    """Pull stablecoin data from Defillama."""
    dfl_pull_stables()
