import typer
from op_coreutils.logger import structlog

from .agora import agora_pull
from .defillama import pull_stables as dfl_pull_stables
from .l2beat import pull as l2beat_pull
from .github_analytics import pull as github_analytics_pull

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


@app.command()
def agora():
    """Pull data from Agora."""
    agora_pull()


@app.command()
def github_analytics():
    """Pull repo analytics data from GitHub."""
    github_analytics_pull()
